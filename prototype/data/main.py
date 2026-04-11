"""--- Import start ---"""

from __future__ import annotations
import pickle
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pandas as pd
import zmq
from _logging import get_logger, run_module
from adaptors import DataSourceAdaptor, YfAdaptor
from sqlalchemy import (
    BigInteger,
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
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
# DB Connection
DB_URL = "postgresql+psycopg2://postgres:password@localhost:6543/postgres"
PUB_ADDR = "tcp://*:5555"
TOPIC_OHLCV = b"OHLCV"

# Asset class definitions
ASSET_CLASS_EQUITY = "EQUITY"
ASSET_CLASS_FX = "FX"


@dataclass(frozen=True)
class InstrumentUniverse:
    """Defines a set of tradable instruments by asset class, market, and ticker symbol,
    along with the adaptor to fetch their OHLCV data from."""

    asset_class: str
    market: str
    adaptor: DataSourceAdaptor
    tickers: list[str] = field(default_factory=list)


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
FX_PAIRS = [
    "EURUSD=X",
    "GBPUSD=X",
    "AUDUSD=X",
    "JPYUSD=X",
]

INSTRUMENT_UNIVERSES: list[InstrumentUniverse] = [
    InstrumentUniverse(
        asset_class=ASSET_CLASS_EQUITY,
        market="US",
        adaptor=YfAdaptor(),
        tickers=US_EQUITIES,
    ),
    InstrumentUniverse(
        asset_class=ASSET_CLASS_EQUITY,
        market="UK",
        adaptor=YfAdaptor(),
        tickers=UK_EQUITIES,
    ),
    InstrumentUniverse(
        asset_class=ASSET_CLASS_FX,
        market="FX",
        adaptor=YfAdaptor(),
        tickers=FX_PAIRS,
    ),
]

# Scope definitions
LOOKBACK_DAYS = 365
PUBLISH_INTERVAL_S = 5.0
"""--- Config end ---"""

log = get_logger("data")


class Base(DeclarativeBase):
    pass


class Instrument(Base):
    """Core CTI table storing one row per unique (asset_class, market, symbol) combination."""

    __tablename__ = "instruments"

    instrument_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    asset_class: Mapped[str] = mapped_column(String(16), nullable=False)
    market: Mapped[str] = mapped_column(String(16), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)

    __table_args__ = (
        UniqueConstraint("asset_class", "market", "symbol", name="uq_instruments_key"),
    )


class EquityDetail(Base):
    """Equity-specific extension; minimal for now, ready to grow (exchange, sector, ISIN…)."""

    __tablename__ = "equity_details"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
        primary_key=True,
    )


class FxDetail(Base):
    """FX-pair-specific extension storing the base/quote currency split."""

    __tablename__ = "fx_details"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
        primary_key=True,
    )
    base_currency: Mapped[str] = mapped_column(String(8), nullable=False)
    quote_currency: Mapped[str] = mapped_column(String(8), nullable=False)


class OHLCV(Base):
    """Daily OHLCV bar keyed by (instrument_id, ts). `ts` is the hypertable partition column."""

    __tablename__ = "ohlcv"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
        primary_key=True,
    )
    ts: Mapped[date] = mapped_column(Date, primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    adj_close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)


def _schema_matches(engine) -> bool:
    """Check whether the existing DB schema matches the ORM models (all tables exist with expected columns)."""
    inspector = inspect(engine)
    existing = set(inspector.get_table_names())
    for table in Base.metadata.sorted_tables:
        if table.name not in existing:
            return False
        expected = {c.name for c in table.columns}
        actual = {c["name"] for c in inspector.get_columns(table.name)}
        if expected != actual:
            return False
    return True


def setup_database():
    """Create tables if they don't exist or if the schema doesn't match, and return an engine connected to the DB."""
    with log.pipeline("db.setup", url=DB_URL):
        engine = create_engine(DB_URL, future=True)
        try:
            if not _schema_matches(engine):
                log.warn(
                    "schema mismatch; rebuilding",
                    tables=[t.name for t in Base.metadata.sorted_tables],
                )
                with engine.begin() as conn:
                    for table in reversed(Base.metadata.sorted_tables):
                        conn.execute(
                            text(f"DROP TABLE IF EXISTS {table.name} CASCADE;")
                        )
            else:
                log.info(
                    "schema ok",
                    tables=len(Base.metadata.sorted_tables),
                )
            Base.metadata.create_all(engine)
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "SELECT create_hypertable('ohlcv', 'ts', "
                        "if_not_exists => TRUE, migrate_data => TRUE);"
                    )
                )
            log.info("hypertable ready", table="ohlcv")
        except OperationalError as e:
            log.error(
                "cannot connect to database",
                url=DB_URL,
                error_type=type(e.orig).__name__,
                error=str(e.orig).splitlines()[0] if str(e.orig) else "",
                hint="is TimescaleDB running and reachable on that host/port?",
            )
            sys.exit(1)
    return engine


def _parse_fx_symbol(symbol: str) -> tuple[str, str]:
    """Parse a FX symbol like 'EURUSD=X' into (base_currency, quote_currency)."""
    core = symbol.removesuffix("=X")
    if len(core) != 6:
        raise ValueError(
            f"cannot parse FX symbol {symbol!r}: expected 6-letter base+quote "
            "(yfinance format like 'EURUSD=X')"
        )
    return core[:3], core[3:]


def _make_detail(universe: InstrumentUniverse, symbol: str):
    """Create an asset-class-specific detail row for this ticker."""
    if universe.asset_class == ASSET_CLASS_EQUITY:
        return EquityDetail()
    if universe.asset_class == ASSET_CLASS_FX:
        base, quote = _parse_fx_symbol(symbol)
        return FxDetail(base_currency=base, quote_currency=quote)
    raise ValueError(f"unknown asset_class: {universe.asset_class!r}")


def ensure_instrument(session, universe: InstrumentUniverse, symbol: str) -> int:
    """Ensure an `instruments` row (plus the asset-class detail row) exists for this ticker,
    and return its instrument_id."""
    existing = session.execute(
        select(Instrument).where(
            and_(
                Instrument.asset_class == universe.asset_class,
                Instrument.market == universe.market,
                Instrument.symbol == symbol,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing.instrument_id

    # Quote currency is deterministic for FX; unknown for equities for now.
    currency: str | None = None
    if universe.asset_class == ASSET_CLASS_FX:
        _, currency = _parse_fx_symbol(symbol)

    instrument = Instrument(
        asset_class=universe.asset_class,
        market=universe.market,
        symbol=symbol,
        currency=currency,
    )
    session.add(instrument)
    session.flush()

    detail = _make_detail(universe, symbol)
    detail.instrument_id = instrument.instrument_id
    session.add(detail)
    session.commit()
    return instrument.instrument_id


def _row_to_orm(instrument_id: int, ts, row) -> OHLCV:
    """Convert a OHLCV row to an ORM object,
    filling in the instrument_id and parsing types."""
    return OHLCV(
        instrument_id=instrument_id,
        ts=ts.date() if hasattr(ts, "date") else ts,
        open=float(row["Open"]),
        high=float(row["High"]),
        low=float(row["Low"]),
        close=float(row["Close"]),
        adj_close=float(row["Adj Close"]),
        volume=int(row["Volume"]),
    )


def find_missing_ranges(
    session, instrument_id: int, start: date, end: date
) -> list[tuple[date, date]]:
    """Return (lo, hi) date tuples covering missing-day gaps in [start, end] for an instrument."""
    min_ts, max_ts = session.execute(
        select(func.min(OHLCV.ts), func.max(OHLCV.ts)).where(
            and_(
                OHLCV.instrument_id == instrument_id,
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
    adaptor: DataSourceAdaptor,
    instrument_id: int,
    symbol: str,
    lo: date,
    hi: date,
) -> list[OHLCV]:
    """Fetch OHLCV rows for [lo, hi] from the adaptor and convert to ORM objects."""
    df = adaptor.fetch_ohlcv(symbol, lo, hi)
    if df.empty:
        return []
    return [_row_to_orm(instrument_id, ts, row) for ts, row in df.iterrows()]


def _prompt_retry(label: str) -> bool:
    """Prompt the user to retry after a fetch error.
    Returns True if they want to retry, False to abort."""
    if not sys.stdin.isatty():
        log.warn("non-interactive; skipping retry", symbol=label)
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
        log.warn("please answer 'y' or 'n'")


def _fetch_with_retry(
    adaptor: DataSourceAdaptor,
    instrument_id: int,
    symbol: str,
    lo: date,
    hi: date,
    label: str,
) -> list[OHLCV] | None:
    """Fetch with retry loop: on error, prompt the user to retry or abort. Returns None if aborted."""
    while True:
        log.debug(
            "fetching range",
            symbol=label,
            lo=lo.isoformat(),
            hi=hi.isoformat(),
            adaptor=adaptor.name,
        )
        try:
            return fetch_range(adaptor, instrument_id, symbol, lo, hi)
        except Exception as e:
            log.exception(
                "fetch failed",
                symbol=label,
                lo=lo.isoformat(),
                hi=hi.isoformat(),
                error_type=type(e).__name__,
            )
            if not _prompt_retry(label):
                return None


def fetch_and_store(
    engine,
    universe: InstrumentUniverse,
    start: date,
    end: date,
) -> None:
    """Fetch missing OHLCV rows for all tickers in this universe and upsert them into the DB."""
    session_factory = sessionmaker(bind=engine, future=True)
    up_to_date = 0
    upserted_total = 0
    warned = 0
    with session_factory() as session:
        for symbol in universe.tickers:
            label = f"{universe.market}:{symbol}"
            instrument_id = ensure_instrument(session, universe, symbol)
            gaps = find_missing_ranges(session, instrument_id, start, end)
            if not gaps:
                up_to_date += 1
                continue

            fetched = 0
            aborted = False
            for lo, hi in gaps:
                rows = _fetch_with_retry(
                    universe.adaptor, instrument_id, symbol, lo, hi, label
                )
                if rows is None:
                    aborted = True
                    break
                for row in rows:
                    session.merge(row)
                fetched += len(rows)

            if fetched == 0:
                warned += 1
                if aborted:
                    log.warn("skipped after fetch error", symbol=label)
                else:
                    log.warn(
                        "no rows returned; check ticker/market suffix",
                        symbol=label,
                        adaptor=universe.adaptor.name,
                    )
            else:
                session.commit()
                upserted_total += fetched
                log.info(
                    "upserted",
                    symbol=label,
                    rows=fetched,
                    partial=aborted,
                )
    log.info(
        "fetch summary",
        asset=universe.asset_class,
        market=universe.market,
        cached=up_to_date,
        upserted_rows=upserted_total,
        warnings=warned,
    )


def load_snapshot(
    engine,
    universes: list[InstrumentUniverse],
    start: date,
    end: date,
) -> dict[str, pd.DataFrame]:
    """Load OHLCV rows for all tickers in the universes and return a snapshot dict."""
    session_factory = sessionmaker(bind=engine, future=True)
    snapshot: dict[str, pd.DataFrame] = {}
    with session_factory() as session:
        for universe in universes:
            for symbol in universe.tickers:
                inst = session.execute(
                    select(Instrument).where(
                        and_(
                            Instrument.asset_class == universe.asset_class,
                            Instrument.market == universe.market,
                            Instrument.symbol == symbol,
                        )
                    )
                ).scalar_one_or_none()
                if inst is None:
                    continue
                rows = (
                    session.execute(
                        select(OHLCV)
                        .where(
                            and_(
                                OHLCV.instrument_id == inst.instrument_id,
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
                label = f"{universe.market}:{symbol}"
                if len(rows) < 2:
                    log.warn(
                        "too few bars; excluding from snapshot",
                        symbol=label,
                        bars=len(rows),
                        window_start=start.isoformat(),
                        window_end=end.isoformat(),
                        reason="need >=2 to form a return",
                    )
                    continue
                closes = [r.adj_close for r in rows]
                if min(closes) == max(closes):
                    log.warn(
                        "constant close; excluding from snapshot",
                        symbol=label,
                        bars=len(rows),
                        reason="would make covariance singular",
                    )
                    continue
                snapshot[label] = pd.DataFrame(
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
    """Main entry point: fetch missing data, load snapshot, and publish it in a loop."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    log.info(
        "lookback window",
        start=start.isoformat(),
        end=end.isoformat(),
        days=LOOKBACK_DAYS,
    )

    engine = setup_database()

    with log.pipeline("fetch", universes=len(INSTRUMENT_UNIVERSES)):
        for universe in INSTRUMENT_UNIVERSES:
            with log.pipeline(
                "universe",
                asset=universe.asset_class,
                market=universe.market,
                tickers=len(universe.tickers),
                adaptor=universe.adaptor.name,
            ):
                fetch_and_store(engine, universe, start, end)

    with log.pipeline("snapshot.load"):
        snapshot: dict[str, pd.DataFrame] = load_snapshot(
            engine, INSTRUMENT_UNIVERSES, start, end
        )
        log.info("snapshot loaded", assets=len(snapshot))
    if not snapshot:
        log.warn("snapshot empty; nothing to publish")
        return

    with log.pipeline("publish.bind", addr=PUB_ADDR):
        ctx, pub = make_publisher()
        log.info("publisher bound", addr=PUB_ADDR, assets=len(snapshot))

    time.sleep(2.0)  # Wait for subscribers to connect before the first message.

    payload = pickle.dumps(
        {
            "tickers": list(snapshot.keys()),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "ohlcv": snapshot,
        }
    )
    log.info(
        "snapshot serialized",
        bytes=len(payload),
        assets=len(snapshot),
    )

    seq = 0
    try:
        while True:
            pub.send_multipart([TOPIC_OHLCV, payload])
            seq += 1
            log.info(
                "published",
                topic=TOPIC_OHLCV.decode(),
                seq=seq,
                assets=len(snapshot),
            )
            time.sleep(PUBLISH_INTERVAL_S)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", published=seq)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    run_module("data", main)
