"""--- Import start ---"""

from __future__ import annotations
import pickle
import signal
import sys
import time
from datetime import date, timedelta
import pandas as pd
import zmq
from adaptors import DataSourceAdaptor, YfAdaptor
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
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

"""--- Import end ---"""

"""--- Config start ---"""
DB_URL = "postgresql+psycopg2://postgres:password@localhost:6543/postgres"
PUB_ADDR = "tcp://*:5555"  # Define Port here
TOPIC_OHLCV = b"OHLCV"

TARGET_TICKERS = [
    # Define targeting assets here
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
"""--- Config end ---"""


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
    """Connet to TimescaleDB and initialize the `ohlcv` hypertable if it doesn't exist."""
    engine = create_engine(DB_URL, future=True)
    try:
        Base.metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "SELECT create_hypertable('ohlcv', 'ts', "
                    "if_not_exists => TRUE, migrate_data => TRUE);"
                )
            )
    except OperationalError as e:
        print(
            f"[data] ERROR: cannot connect to database at {DB_URL}\n"
            f"[data]   reason: {e.orig}\n"
            f"[data]   hint:   is TimescaleDB running and reachable on that host/port?",
            file=sys.stderr,
        )
        sys.exit(1)
    return engine


def _row_to_orm(ticker: str, ts, row) -> OHLCV:
    """Build one OHLCV ORM instance from a canonical OHLCV index/row pair."""
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
    """Return a list of (lo, hi) date tuples covering any missing-day gaps in [start, end]."""
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


def fetch_range(
    adaptor: DataSourceAdaptor, ticker: str, lo: date, hi: date
) -> list[OHLCV]:
    """Pull [lo, hi] from the given data source adaptor and return ORM rows (empty if none)."""
    df = adaptor.fetch_ohlcv(ticker, lo, hi)
    if df.empty:
        return []
    return [_row_to_orm(ticker, ts, row) for ts, row in df.iterrows()]


def fetch_and_store(
    engine,
    adaptor: DataSourceAdaptor,
    tickers: list[str],
    start: date,
    end: date,
) -> None:
    """For each ticker, fill any missing-day gaps in [start, end] from the given adaptor."""
    session = sessionmaker(bind=engine, future=True)
    with session() as session:
        for ticker in tickers:
            gaps = find_missing_ranges(session, ticker, start, end)
            if not gaps:
                print(f"[data] {ticker}: cache up to date")
                continue

            fetched = 0
            for lo, hi in gaps:
                print(f"[data] {ticker}: fetching {lo} → {hi} via {adaptor.name}")
                rows = fetch_range(adaptor, ticker, lo, hi)
                for row in rows:
                    session.merge(row)
                fetched += len(rows)
            session.commit()
            print(f"[data] {ticker}: upserted {fetched} row(s)")


def load_snapshot(
    engine, tickers: list[str], start: date, end: date
) -> dict[str, pd.DataFrame]:
    """Read all cached OHLCV rows in [start, end] back into a {ticker: DataFrame} mapping."""
    session = sessionmaker(bind=engine, future=True)
    snapshot: dict[str, pd.DataFrame] = {}
    with session() as session:
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
    """Main entry point: ensure DB is up to date, load snapshot, and publish on a PUB socket."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    # Get the range of past 12 months
    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)

    # Ensure the database is up to date, then load the snapshot to publish
    engine = setup_database()
    adaptor: DataSourceAdaptor = YfAdaptor()
    fetch_and_store(engine, adaptor, TARGET_TICKERS, start, end)
    snapshot = load_snapshot(engine, TARGET_TICKERS, start, end)

    if not snapshot:
        print("[data] snapshot is empty; nothing to publish")
        return

    ctx, pub = make_publisher()
    print(f"[data] publisher bound to {PUB_ADDR}; tickers={list(snapshot.keys())}")

    time.sleep(2.0)  # Wait for subscribers to connect before sending the first message

    payload = pickle.dumps(
        {
            "tickers": list(snapshot.keys()),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "ohlcv": snapshot,
        }
    )

    # Publish the snapshot
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
