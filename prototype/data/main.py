"""Data module: fetch OHLCV from yfinance into TimescaleDB and publish snapshots over ZMQ."""

from __future__ import annotations

import pickle
import signal
import time
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
import zmq
from sqlalchemy import (
    BigInteger,
    Date,
    Float,
    String,
    and_,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

DB_URL = "postgresql+psycopg2://postgres:password@localhost:6543/postgres"
PUB_ADDR = "tcp://*:5555"
TOPIC_OHLCV = b"OHLCV"

TARGET_TICKERS = [
    "AAPL",
    "GOOG",
    "AMZN",
    "MSFT",
    "TSLA",
    "META",
    "NVDA",
    "JPM",
    "PLTR",
    "INTC",
    "AMD",
    "NFLX",
    "MU",
    "RKLB",
]
LOOKBACK_DAYS = 365
PUBLISH_INTERVAL_S = 5.0


class Base(DeclarativeBase):
    pass


class OHLCV(Base):
    """One row per (ticker, trading day); the table is a TimescaleDB hypertable on `ts`."""

    __tablename__ = "ohlcv"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    ts: Mapped[date] = mapped_column(Date, primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    adj_close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)


def setup_database():
    """Connect, create the OHLCV table, and convert it to a hypertable (idempotent)."""
    engine = create_engine(DB_URL, future=True)
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                "SELECT create_hypertable('ohlcv', 'ts', "
                "if_not_exists => TRUE, migrate_data => TRUE);"
            )
        )
    return engine


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the multi-index column wrapping yfinance adds for single-ticker downloads."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] for c in df.columns]
    return df


def _row_to_orm(ticker: str, ts, row) -> OHLCV:
    """Build one OHLCV ORM instance from a yfinance index/row pair."""
    return OHLCV(
        ticker=ticker,
        ts=ts.date() if hasattr(ts, "date") else ts,
        open=float(row["Open"]),
        high=float(row["High"]),
        low=float(row["Low"]),
        close=float(row["Close"]),
        adj_close=float(row["Adj Close"]),
        volume=int(row["Volume"]),
    )


def find_missing_ranges(
    session, ticker: str, start: date, end: date
) -> list[tuple[date, date]]:
    """Return inclusive (lo, hi) gaps inside [start, end] for which the cache has no rows.

    Only the leading gap (before the earliest cached row) and the trailing gap
    (after the latest cached row) are considered. Holes between two existing
    trading days are ignored on purpose: yfinance never returns bars for
    weekends or holidays, so an "interior hole" is normal and re-fetching it
    would be wasteful.
    """
    min_ts, max_ts = session.execute(
        select(func.min(OHLCV.ts), func.max(OHLCV.ts)).where(
            and_(OHLCV.ticker == ticker, OHLCV.ts >= start, OHLCV.ts <= end)
        )
    ).one()

    if min_ts is None:
        return [(start, end)]

    gaps: list[tuple[date, date]] = []
    if start < min_ts:
        gaps.append((start, min_ts - timedelta(days=1)))
    if max_ts < end:
        gaps.append((max_ts + timedelta(days=1), end))
    return gaps


def fetch_range(ticker: str, lo: date, hi: date) -> list[OHLCV]:
    """Download [lo, hi] inclusive from yfinance and return ORM rows (empty if none)."""
    df = yf.download(
        ticker,
        start=lo.isoformat(),
        end=(hi + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
        auto_adjust=False,
        progress=False,
    )
    if df is None or df.empty:
        return []
    df = _flatten_columns(df)
    return [_row_to_orm(ticker, ts, row) for ts, row in df.iterrows()]


def fetch_and_store(engine, tickers: list[str], start: date, end: date) -> None:
    """For each ticker, fill any missing-day gaps in [start, end] from yfinance."""
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        for ticker in tickers:
            gaps = find_missing_ranges(session, ticker, start, end)
            if not gaps:
                print(f"[data] {ticker}: cache up to date")
                continue

            fetched = 0
            for lo, hi in gaps:
                print(f"[data] {ticker}: fetching {lo} → {hi}")
                rows = fetch_range(ticker, lo, hi)
                for row in rows:
                    session.merge(row)
                fetched += len(rows)
            session.commit()
            print(f"[data] {ticker}: upserted {fetched} row(s)")


def load_snapshot(
    engine, tickers: list[str], start: date, end: date
) -> dict[str, pd.DataFrame]:
    """Read all cached OHLCV rows in [start, end] back into a {ticker: DataFrame} mapping."""
    Session = sessionmaker(bind=engine, future=True)
    snapshot: dict[str, pd.DataFrame] = {}
    with Session() as session:
        for ticker in tickers:
            rows = (
                session.execute(
                    select(OHLCV)
                    .where(
                        and_(OHLCV.ticker == ticker, OHLCV.ts >= start, OHLCV.ts <= end)
                    )
                    .order_by(OHLCV.ts)
                )
                .scalars()
                .all()
            )
            if not rows:
                continue
            snapshot[ticker] = pd.DataFrame(
                [
                    {
                        "ts": r.ts,
                        "open": r.open,
                        "high": r.high,
                        "low": r.low,
                        "close": r.close,
                        "adj_close": r.adj_close,
                        "volume": r.volume,
                    }
                    for r in rows
                ]
            ).set_index("ts")
    return snapshot


def make_publisher() -> tuple[zmq.Context, zmq.Socket]:
    """Bind a PUB socket on PUB_ADDR and return (context, socket)."""
    ctx = zmq.Context.instance()
    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)
    return ctx, pub


def main() -> None:
    """Update the cache for the trailing 12 months, then loop forever broadcasting it."""
    # Make SIGTERM raise KeyboardInterrupt so the finally block below runs
    # and the PUB socket is closed cleanly before the process exits.
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)

    engine = setup_database()
    fetch_and_store(engine, TARGET_TICKERS, start, end)
    snapshot = load_snapshot(engine, TARGET_TICKERS, start, end)

    if not snapshot:
        print("[data] snapshot is empty; nothing to publish")
        return

    ctx, pub = make_publisher()
    print(f"[data] publisher bound to {PUB_ADDR}; tickers={list(snapshot.keys())}")

    time.sleep(2.0)  # let subscribers connect before the first send

    payload = pickle.dumps(
        {
            "tickers": list(snapshot.keys()),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "ohlcv": snapshot,
        }
    )

    try:
        while True:
            pub.send_multipart([TOPIC_OHLCV, payload])
            print("[data] published OHLCV snapshot")
            time.sleep(PUBLISH_INTERVAL_S)
    except KeyboardInterrupt:
        pass
    finally:
        print("[data] closing sockets")
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
