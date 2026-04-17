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
from _logging import run_module
from adaptors import DataSourceAdaptor, YfAdaptor
from snapshots import emit_data_snapshot, emit_data_trace, log
from sqlalchemy import (
    BigInteger,
    Date,
    Engine,
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
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

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
INITIAL_SUBSCRIBER_GRACE_S = 2.0

# Logging
CYCLE_LOG_EVERY_N = 50
"""--- Config end ---"""


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
    """Check whether the existing DB schema matches the ORM models."""
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
            # Check table schemas
            if not _schema_matches(engine):
                log.warn(
                    "schema mismatch; rebuilding",
                    tables=[t.name for t in Base.metadata.sorted_tables],
                )

                # Drop tables
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

            # Create tables
            Base.metadata.create_all(engine)
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "SELECT create_hypertable('ohlcv', 'ts', "
                        "if_not_exists => TRUE, migrate_data => TRUE);"
                    )
                )

            log.info("hypertable ready", table="ohlcv")
        # Connection Failed
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


def ensure_instrument(
    session: Session, universe: InstrumentUniverse, symbol: str
) -> int:
    """Ensure an `instruments` row (plus the asset-class detail row) exists for this ticker,
    and return its instrument_id. The caller is responsible for committing."""
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

    if universe.asset_class == ASSET_CLASS_EQUITY:
        detail = EquityDetail(instrument_id=instrument.instrument_id)
    elif universe.asset_class == ASSET_CLASS_FX:
        base, quote = _parse_fx_symbol(symbol)
        detail = FxDetail(
            instrument_id=instrument.instrument_id,
            base_currency=base,
            quote_currency=quote,
        )
    else:
        raise ValueError(f"unknown asset_class: {universe.asset_class!r}")
    session.add(detail)
    return instrument.instrument_id


def fetch_and_store(
    engine: Engine,
    universe: InstrumentUniverse,
    start: date,
    end: date,
) -> dict[str, int]:
    """Fetch OHLCV bars missing between ``[start, end]`` and upsert them.

    For every symbol we resume from the day after our latest bar in the
    window (or from ``start`` if we have nothing yet). All symbols that
    still need data are fetched in one batch covering the earliest required
    day through ``end``; yfinance is idempotent on overlap and ``merge``
    swallows same-valued rows.
    """
    stats = {"cached": 0, "fetched": 0, "upserted": 0, "warnings": 0, "errors": 0}
    session_factory = sessionmaker(bind=engine, future=True)

    with session_factory() as session:
        ids = {s: ensure_instrument(session, universe, s) for s in universe.tickers}
        session.commit()

        last_ts = dict(
            session.execute(
                select(OHLCV.instrument_id, func.max(OHLCV.ts))
                .where(
                    and_(
                        OHLCV.instrument_id.in_(ids.values()),
                        OHLCV.ts >= start,
                        OHLCV.ts <= end,
                    )
                )
                .group_by(OHLCV.instrument_id)
            ).all()
        )

        to_fetch: list[tuple[str, int]] = []
        fetch_windows: list[date] = []
        for symbol, iid in ids.items():
            prev = last_ts.get(iid)
            if prev is not None and prev >= end:
                stats["cached"] += 1
                continue
            to_fetch.append((symbol, iid))
            fetch_windows.append(prev + timedelta(days=1) if prev else start)

        if not to_fetch:
            return stats

        symbols = [s for s, _ in to_fetch]
        batch_start = min(fetch_windows)
        log.debug(
            "batch fetch",
            market=universe.market,
            symbols=len(symbols),
            start=batch_start.isoformat(),
            end=end.isoformat(),
            adaptor=universe.adaptor.name,
        )

        try:
            result = universe.adaptor.fetch_ohlcv_many(symbols, batch_start, end)
        except Exception as e:
            log.exception(
                "batch fetch failed",
                market=universe.market,
                symbols=len(symbols),
                error_type=type(e).__name__,
            )
            stats["errors"] += len(symbols)
            return stats

        stats["fetched"] = len(to_fetch)
        for symbol, iid in to_fetch:
            df = result.get(symbol)
            label = f"{universe.market}:{symbol}"
            if df is None or df.empty:
                log.warn(
                    "no rows returned; check ticker/market suffix",
                    symbol=label,
                    adaptor=universe.adaptor.name,
                )
                stats["warnings"] += 1
                continue
            for ts, row in df.iterrows():
                session.merge(
                    OHLCV(
                        instrument_id=iid,
                        ts=ts.date() if hasattr(ts, "date") else ts,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        adj_close=float(row["Adj Close"]),
                        volume=int(row["Volume"]),
                    )
                )
            stats["upserted"] += len(df)
            log.debug("upserted", symbol=label, rows=len(df))

        session.commit()

    return stats


def load_snapshot(
    engine: Engine,
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


def _run_cycle(
    engine: Engine,
    universes: list[InstrumentUniverse],
    start: date,
    end: date,
) -> tuple[dict[str, int], dict[str, pd.DataFrame]]:
    """Run one fetch+snapshot cycle for the given universes and date window, returning stats and the snapshot."""
    total = {"cached": 0, "fetched": 0, "upserted": 0, "warnings": 0, "errors": 0}

    for universe in universes:
        stats = fetch_and_store(engine, universe, start, end)
        for k, v in stats.items():
            total[k] = total.get(k, 0) + v
    snapshot = load_snapshot(engine, universes, start, end)

    return total, snapshot


def main() -> None:
    """Main entry point: run one bootstrap cycle, then loop over fetch+snapshot+publish."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    log.info("lookback configured", days=LOOKBACK_DAYS)

    engine = setup_database()

    # Bootstrap with an initial fetch+snapshot to populate the DB
    with log.pipeline("bootstrap", universes=len(INSTRUMENT_UNIVERSES)):
        end = date.today()
        start = end - timedelta(days=LOOKBACK_DAYS)
        log.info(
            "window",
            start=start.isoformat(),
            end=end.isoformat(),
        )

        # Run a bootstrap cycle
        stats, snapshot = _run_cycle(engine, INSTRUMENT_UNIVERSES, start, end)
        log.info(
            "bootstrap fetch",
            assets=len(snapshot),
            **stats,
        )

        # Observations
        if stats["warnings"] or stats["errors"]:
            log.warn(
                "bootstrap had problems",
                warnings=stats["warnings"],
                errors=stats["errors"],
            )

    if not snapshot:
        log.error("bootstrap snapshot empty; aborting")
        sys.exit(1)

    with log.pipeline("publish.bind", addr=PUB_ADDR):
        ctx, pub = make_publisher()
    time.sleep(INITIAL_SUBSCRIBER_GRACE_S)

    seq = 0
    try:
        while True:
            # Increment the counter and computes a date window
            seq += 1
            t0 = time.monotonic()
            end = date.today()
            start = end - timedelta(days=LOOKBACK_DAYS)

            # Fetch data
            stats, snapshot = _run_cycle(engine, INSTRUMENT_UNIVERSES, start, end)
            if not snapshot:
                log.warn("cycle snapshot empty", seq=seq)
                continue

            # Publish via ZeroMQ
            payload = pickle.dumps(
                {
                    "tickers": list(snapshot.keys()),
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "ohlcv": snapshot,
                }
            )
            pub.send_multipart([TOPIC_OHLCV, payload])

            # Observations
            dur_ms = (time.monotonic() - t0) * 1000.0
            emit_data_snapshot(seq, start, end, stats, snapshot, len(payload), dur_ms)
            emit_data_trace(seq, start, end, snapshot)
            changed = (
                stats["upserted"] > 0 or stats["warnings"] > 0 or stats["errors"] > 0
            )
            noteworthy = changed or seq == 1 or (seq % CYCLE_LOG_EVERY_N == 0)
            emit = log.info if noteworthy else log.debug
            emit(
                "cycle",
                seq=seq,
                duration_ms=f"{dur_ms:.1f}",
                assets=len(snapshot),
                bytes=len(payload),
                **stats,
            )
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", published=seq)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    run_module("data", main)
