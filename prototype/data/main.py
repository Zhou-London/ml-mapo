"""--- Import start ---"""

from __future__ import annotations
import pickle
import signal
import sys
import time
from dataclasses import dataclass, field
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
    inspect,
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


@dataclass(frozen=True)
class MarketUniverse:
    """Defines a market universe to fetch and publish."""

    market: str
    adaptor: DataSourceAdaptor
    tickers: list[str] = field(default_factory=list)


# US equities
US_EQUITIES = [
    "NVDA",
    "AAPL",
    "MSFT",
    "AMZN",
    "GOOGL",
    "GOOG",
    "AVGO",
    "META",
    "TSLA",
    "WMT",
    "ASML",
    "MU",
    "COST",
    "NFLX",
    "AMD",
    "LRCX",
    "CSCO",
    "AMAT",
    "INTC",
    "PLTR",
    "LIN",
    "KLAC",
    "TMUS",
    "PEP",
    "TXN",
    "AMGN",
    "GILD",
    "ADI",
    "ISRG",
    "ARM",
    "HON",
    "SHOP",
    "PDD",
    "BKNG",
    "QCOM",
    "APP",
    "PANW",
    "WDC",
    "STX",
    "MRVL",
    "VRTX",
    "SBUX",
    "CEG",
    "CMCSA",
    "INTU",
    "CRWD",
    "MAR",
    "ADBE",
    "MELI",
    "REGN",
]

UK_EQUITIES = ["HSBA.L"]

MARKET_UNIVERSES: list[MarketUniverse] = [
    MarketUniverse(market="US", adaptor=YfAdaptor(), tickers=US_EQUITIES),
    MarketUniverse(market="UK", adaptor=YfAdaptor(), tickers=UK_EQUITIES),
]

LOOKBACK_DAYS = 365
PUBLISH_INTERVAL_S = 5.0
"""--- Config end ---"""


class Base(DeclarativeBase):
    pass


class OHLCV(Base):
    """One row per (market, ticker, trading day). `ts` is the hypertable time column."""

    __tablename__ = "ohlcv"

    market: Mapped[str] = mapped_column(String(16), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(32), primary_key=True)
    ts: Mapped[date] = mapped_column(Date, primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    adj_close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)


def setup_database():
    """Connect to TimescaleDB and initialize the `ohlcv` hypertable if it doesn't exist."""
    engine = create_engine(DB_URL, future=True)
    try:
        inspector = inspect(engine)
        if inspector.has_table("ohlcv"):
            cols = {c["name"] for c in inspector.get_columns("ohlcv")}
            if "market" not in cols:
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE ohlcv CASCADE;"))
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


def _row_to_orm(market: str, ticker: str, ts, row) -> OHLCV:
    """Build one OHLCV ORM instance from a canonical OHLCV index/row pair."""
    return OHLCV(
        market=market,
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
    session, market: str, ticker: str, start: date, end: date
) -> list[tuple[date, date]]:
    """Return a list of (lo, hi) date tuples covering any missing-day gaps in [start, end]."""
    min_ts, max_ts = session.execute(
        select(func.min(OHLCV.ts), func.max(OHLCV.ts)).where(
            and_(
                OHLCV.market == market,
                OHLCV.ticker == ticker,
                OHLCV.ts >= start,
                OHLCV.ts <= end,
            )
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
    market: str,
    adaptor: DataSourceAdaptor,
    ticker: str,
    lo: date,
    hi: date,
) -> list[OHLCV]:
    """Pull [lo, hi] from the given data source adaptor and return ORM rows (empty if none)."""
    df = adaptor.fetch_ohlcv(ticker, lo, hi)
    if df.empty:
        return []
    return [_row_to_orm(market, ticker, ts, row) for ts, row in df.iterrows()]


def _prompt_retry(label: str) -> bool:
    """Ask the user whether to retry a failed fetch. Auto-skip in non-interactive sessions."""
    if not sys.stdin.isatty():
        print(
            f"[data] {label}: non-interactive session, skipping retry",
            file=sys.stderr,
        )
        return False
    while True:
        try:
            answer = input(f"[data] {label}: retry fetch? [y/N] ").strip().lower()
        except (EOFError, OSError):
            return False
        if answer in ("", "n", "no"):
            return False
        if answer in ("y", "yes"):
            return True
        print("[data] please answer 'y' or 'n'", file=sys.stderr)


def _fetch_with_retry(
    market: str,
    adaptor: DataSourceAdaptor,
    ticker: str,
    lo: date,
    hi: date,
    label: str,
) -> list[OHLCV] | None:
    """Fetch [lo, hi] with interactive retry on adaptor error. Returns None if aborted."""
    while True:
        print(f"[data] {label}: fetching {lo} → {hi} via {adaptor.name}")
        try:
            return fetch_range(market, adaptor, ticker, lo, hi)
        except Exception as e:
            print(
                f"[data] ERROR: {label}: fetch failed "
                f"({type(e).__name__}: {e})",
                file=sys.stderr,
            )
            if not _prompt_retry(label):
                return None


def fetch_and_store(
    engine,
    universe: MarketUniverse,
    start: date,
    end: date,
) -> None:
    """Fill missing-day gaps in [start, end] for every ticker in the universe."""
    session = sessionmaker(bind=engine, future=True)
    with session() as session:
        for ticker in universe.tickers:
            label = f"{universe.market}:{ticker}"
            gaps = find_missing_ranges(session, universe.market, ticker, start, end)
            if not gaps:
                print(f"[data] {label}: cache up to date")
                continue

            fetched = 0
            aborted = False
            for lo, hi in gaps:
                rows = _fetch_with_retry(
                    universe.market, universe.adaptor, ticker, lo, hi, label
                )
                if rows is None:
                    aborted = True
                    break
                for row in rows:
                    session.merge(row)
                fetched += len(rows)

            if fetched == 0:
                if aborted:
                    print(
                        f"[data] WARNING: {label}: skipped after fetch error",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"[data] WARNING: {label}: no rows returned from "
                        f"{universe.adaptor.name} — check ticker symbol / market suffix",
                        file=sys.stderr,
                    )
            else:
                session.commit()
                suffix = " (partial, after fetch error)" if aborted else ""
                print(f"[data] {label}: upserted {fetched} row(s){suffix}")


def load_snapshot(
    engine,
    universes: list[MarketUniverse],
    start: date,
    end: date,
) -> dict[str, pd.DataFrame]:
    """Read cached OHLCV rows in [start, end] into a {"market:ticker": DataFrame} mapping."""
    session = sessionmaker(bind=engine, future=True)
    snapshot: dict[str, pd.DataFrame] = {}
    with session() as session:
        for universe in universes:
            for ticker in universe.tickers:
                rows = (
                    session.execute(
                        select(OHLCV)
                        .where(
                            and_(
                                OHLCV.market == universe.market,
                                OHLCV.ticker == ticker,
                                OHLCV.ts >= start,
                                OHLCV.ts <= end,
                            )
                        )
                        .order_by(OHLCV.ts)
                    )
                    .scalars()
                    .all()
                )
                if not rows:
                    continue
                snapshot[f"{universe.market}:{ticker}"] = pd.DataFrame(
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

    # Connect to database
    engine = setup_database()

    # Fill missing days for every market universe
    for universe in MARKET_UNIVERSES:
        print(
            f"[data] universe {universe.market}: "
            f"{len(universe.tickers)} ticker(s) via {universe.adaptor.name}"
        )
        fetch_and_store(engine, universe, start, end)

    snapshot: dict[str, pd.DataFrame] = load_snapshot(
        engine, MARKET_UNIVERSES, start, end
    )
    if not snapshot:
        print("[data] snapshot is empty; nothing to publish")
        return

    ctx, pub = make_publisher()
    print(f"[data] publisher bound to {PUB_ADDR}; assets={list(snapshot.keys())}")

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
