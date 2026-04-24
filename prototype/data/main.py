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
from adaptors import BondAdaptor, DataSourceAdaptor, YfAdaptor
from bond_adaptors import (
    EU_BOND_TICKERS,
    UK_BOND_TICKERS,
    US_BOND_TICKERS,
    YfBondAdaptor,
)
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
DB_URL = "postgresql+psycopg2://postgres:6602@localhost:5434/postgres"
PUB_ADDR = "tcp://*:5555"
TOPIC_OHLCV = b"OHLCV"

# Asset class definitions
ASSET_CLASS_EQUITY = "EQUITY"
ASSET_CLASS_FX     = "FX"
ASSET_CLASS_BOND   = "BOND"


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
# ---------------------------------------------------------------------------
# Bond tickers — sourced from bond_adaptors.py
# ---------------------------------------------------------------------------
# US_BOND_TICKERS  : SGOV, BIL, SHY, VGSH, IEF, VGIT, TLH, TLT, VGLT, GOVT
# UK_BOND_TICKERS  : IGLT.L, VGOV.L, GILS.L, GLTY.L, IGLS.L, GLTA.L, INXG.L
# EU_BOND_TICKERS  : IEGA.L, VETY.L, EXX6.DE, IS04.L, EXVM.DE, IBCI.L, IBGL.L

_bond_adaptor_us = YfBondAdaptor()
_bond_adaptor_uk = YfBondAdaptor()
_bond_adaptor_eu = YfBondAdaptor()

INSTRUMENT_UNIVERSES: list[InstrumentUniverse] = [
    # ── Equities ──────────────────────────────────────────────────────────
    InstrumentUniverse(
        asset_class=ASSET_CLASS_EQUITY,
        market="US",
        adaptor=YfAdaptor(asset_class=ASSET_CLASS_EQUITY),
        tickers=US_EQUITIES,
    ),
    InstrumentUniverse(
        asset_class=ASSET_CLASS_EQUITY,
        market="UK",
        adaptor=YfAdaptor(asset_class=ASSET_CLASS_EQUITY),
        tickers=UK_EQUITIES,
    ),
    # ── FX ────────────────────────────────────────────────────────────────
    InstrumentUniverse(
        asset_class=ASSET_CLASS_FX,
        market="FX",
        adaptor=YfAdaptor(asset_class=ASSET_CLASS_FX),
        tickers=FX_PAIRS,
    ),
    # ── Bonds ─────────────────────────────────────────────────────────────
    InstrumentUniverse(
        asset_class=ASSET_CLASS_BOND,
        market="US",
        adaptor=_bond_adaptor_us,
        tickers=US_BOND_TICKERS,
    ),
    InstrumentUniverse(
        asset_class=ASSET_CLASS_BOND,
        market="UK",
        adaptor=_bond_adaptor_uk,
        tickers=UK_BOND_TICKERS,
    ),
    InstrumentUniverse(
        asset_class=ASSET_CLASS_BOND,
        market="EU",
        adaptor=_bond_adaptor_eu,
        tickers=EU_BOND_TICKERS,
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


class BondDetail(Base):
    """Bond-specific static attributes — one row per instrument."""

    __tablename__ = "bond_details"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
        primary_key=True,
    )
    isin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    issuer: Mapped[str | None] = mapped_column(String(128), nullable=True)
    maturity_bucket: Mapped[str | None] = mapped_column(String(32), nullable=True)
    credit_rating: Mapped[str | None] = mapped_column(String(16), nullable=True)
    coupon_rate: Mapped[float | None] = mapped_column(Float, nullable=True)   # decimal
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    region: Mapped[str | None] = mapped_column(String(8), nullable=True)      # US/UK/EU


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


class BondBar(Base):
    """Daily bond bar keyed by (instrument_id, ts).

    Stores the MAPIS Fixed Income critical fields (★) plus supplementary
    fields needed by the risk and alpha layers.
    ts is the hypertable partition column.
    """

    __tablename__ = "bond_bar"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
        primary_key=True,
    )
    ts: Mapped[date] = mapped_column(Date, primary_key=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    # "yield" is a Python keyword — SQLAlchemy column alias used here
    yield_to_maturity: Mapped[float | None] = mapped_column(
        "yield_to_maturity", Float, nullable=True
    )                                                                # ★ MAPIS
    yield_change: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )                                                                # ★ MAPIS
    spread_vs_benchmark: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )                                                                # ★ MAPIS
    duration: Mapped[float | None] = mapped_column(Float, nullable=True)
    coupon_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    rolling_vol_21d: Mapped[float | None] = mapped_column(Float, nullable=True)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------


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
                        "SELECT create_hypertable('ohlcv', by_range('ts'::name), "
                        "if_not_exists => TRUE, migrate_data => TRUE);"
                    )
                )
                conn.execute(
                    text(
                        "SELECT create_hypertable('bond_bar', by_range('ts'::name), "
                        "if_not_exists => TRUE, migrate_data => TRUE);"
                    )
                )
            log.info("hypertables ready", tables=["ohlcv", "bond_bar"])
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
    if universe.asset_class == ASSET_CLASS_BOND:
        from bond_adaptors import _ALL_ETF_UNIVERSE
        meta = _ALL_ETF_UNIVERSE.get(symbol)
        if meta:
            # 8-tuple: (desc, bucket, isin, issuer, coupon_pct, rating, currency, region)
            return BondDetail(
                isin=meta[2],
                issuer=meta[3],
                maturity_bucket=meta[1],
                credit_rating=meta[5],
                coupon_rate=meta[4] / 100.0,  # store as decimal
                currency=meta[6],
                region=meta[7],
            )
        return BondDetail()
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


def _row_to_bond_orm(instrument_id: int, ts, row) -> BondBar:
    """Convert a DataFrame row to a BondBar ORM object.

    All MAPIS fields are nullable — a NaN from the adaptor is stored as NULL
    rather than failing the whole fetch.
    """
    def _f(key: str) -> float | None:
        v = row.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return None if (f != f) else f   # NaN check
        except (TypeError, ValueError):
            return None

    return BondBar(
        instrument_id=instrument_id,
        ts=ts.date() if hasattr(ts, "date") else ts,
        price=_f("price"),
        yield_to_maturity=_f("yield_to_maturity"),
        yield_change=_f("yield_change"),
        spread_vs_benchmark=_f("spread_vs_benchmark"),
        duration=_f("duration"),
        coupon_rate=_f("coupon_rate"),
        rolling_vol_21d=_f("rolling_vol_21d"),
    )


# ---------------------------------------------------------------------------
# Gap detection
# ---------------------------------------------------------------------------


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


def find_missing_ranges_bond(
    session, instrument_id: int, start: date, end: date
) -> list[tuple[date, date]]:
    """Return (lo, hi) tuples covering missing-day gaps in [start, end] for BondBar."""
    min_ts, max_ts = session.execute(
        select(func.min(BondBar.ts), func.max(BondBar.ts)).where(
            and_(
                BondBar.instrument_id == instrument_id,
                BondBar.ts >= start,
                BondBar.ts <= end,
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
) -> list[OHLCV] | list[BondBar]:
    """Fetch bars via the unified fetch_data() interface and return ORM objects."""
    df = adaptor.fetch_data(symbol, lo, hi)
    if df.empty:
        return []
    if adaptor.asset_class == ASSET_CLASS_BOND:
        return [_row_to_bond_orm(instrument_id, ts, row) for ts, row in df.iterrows()]
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
) -> list[OHLCV] | list[BondBar] | None:
    """Fetch with retry loop. Returns None if the user aborts."""
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
    """Fetch missing bars for all tickers in *universe* and upsert into the DB.

    Routes gap detection and ORM conversion to the correct table (OHLCV or
    BondBar) based on universe.asset_class.  Equity/FX paths are unchanged.
    """
    is_bond = universe.asset_class == ASSET_CLASS_BOND
    session_factory = sessionmaker(bind=engine, future=True)
    up_to_date = 0
    upserted_total = 0
    warned = 0
    with session_factory() as session:
        for symbol in universe.tickers:
            label = f"{universe.market}:{symbol}"
            instrument_id = ensure_instrument(session, universe, symbol)

            gaps = (
                find_missing_ranges_bond(session, instrument_id, start, end)
                if is_bond
                else find_missing_ranges(session, instrument_id, start, end)
            )
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


# ---------------------------------------------------------------------------
# Snapshot  (nested by asset class)
# ---------------------------------------------------------------------------


def _ac_key(asset_class: str) -> str:
    """Normalise DB asset-class string ("EQUITY") to snapshot key ("equity").

    The alpha service uses lowercase keys.  DB values stay uppercase.
    """
    return asset_class.lower()


def load_snapshot(
    engine,
    universes: list[InstrumentUniverse],
    start: date,
    end: date,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Load bars for all tickers and return a nested multi-asset snapshot.

    Structure:
        {
            "equity": { "US:AAPL": DataFrame, ... },
            "fx":     { "FX:EURUSD=X": DataFrame, ... },
            "bond":   { "US:TLT": DataFrame, "UK:IGLT.L": DataFrame, ... },
        }

    Each inner DataFrame is indexed by date.
    Equity/FX DataFrames carry OHLCV columns.
    Bond DataFrames carry MAPIS columns (yield_to_maturity, yield_change, etc.).
    Tickers with fewer than 2 bars or zero variance are excluded with a warning.
    """
    session_factory = sessionmaker(bind=engine, future=True)
    snapshot: dict[str, dict[str, pd.DataFrame]] = {}

    with session_factory() as session:
        for universe in universes:
            ac_key = _ac_key(universe.asset_class)
            bucket = snapshot.setdefault(ac_key, {})
            is_bond = universe.asset_class == ASSET_CLASS_BOND

            for symbol in universe.tickers:
                label = f"{universe.market}:{symbol}"
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

                if is_bond:
                    rows = (
                        session.execute(
                            select(BondBar)
                            .where(
                                and_(
                                    BondBar.instrument_id == inst.instrument_id,
                                    BondBar.ts >= start,
                                    BondBar.ts <= end,
                                )
                            )
                            .order_by(BondBar.ts)
                        )
                        .scalars()
                        .all()
                    )
                    if not rows:
                        continue
                    if len(rows) < 2:
                        log.warn(
                            "too few bars; excluding from snapshot",
                            symbol=label,
                            bars=len(rows),
                            window_start=start.isoformat(),
                            window_end=end.isoformat(),
                            reason="need >=2 to form a yield change",
                        )
                        continue
                    # yields = [r.yield_to_maturity for r in rows
                    #           if r.yield_to_maturity is not None]
                    # if yields and min(yields) == max(yields):
                    #     log.warn(
                    #         "constant yield; excluding from snapshot",
                    #         symbol=label,
                    #         bars=len(rows),
                    #         reason="zero variance; useless for alpha",
                    #     )
                    #     continue
                    # AFTER
                    changes = [r.yield_change for r in rows if r.yield_change is not None]
                    if not changes or len(set(changes)) == 1:
                        log.warn(
                            "constant yield_change; excluding from snapshot",
                            symbol=label,
                            bars=len(rows),
                            reason="zero variance; useless for alpha",
                        )
                        continue
                    # Build DataFrame with MAPIS column names — matches BOND_COLUMNS
                    bucket[label] = pd.DataFrame(
                        [
                            {
                                "ts":                  r.ts,
                                "price":               r.price,
                                "yield_to_maturity":   r.yield_to_maturity,
                                "yield_change":        r.yield_change,
                                "spread_vs_benchmark": r.spread_vs_benchmark,
                                "duration":            r.duration,
                                "coupon_rate":         r.coupon_rate,
                                "rolling_vol_21d":     r.rolling_vol_21d,
                            }
                            for r in rows
                        ]
                    ).set_index("ts")

                else:
                    # ── Equity / FX path — identical to original ──────────────
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
                    bucket[label] = pd.DataFrame(
                        [
                            {
                                "ts":        r.ts,
                                "open":      r.open,
                                "high":      r.high,
                                "low":       r.low,
                                "close":     r.close,
                                "Adj Close": r.adj_close,
                                "volume":    r.volume,
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
    # end = date.today() - timedelta(days=2)
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
        snapshot: dict[str, dict[str, pd.DataFrame]] = load_snapshot(
            engine, INSTRUMENT_UNIVERSES, start, end
        )
        total_assets = sum(len(v) for v in snapshot.values())
        log.info(
            "snapshot loaded",
            asset_classes=list(snapshot.keys()),
            total_assets=total_assets,
            per_class={ac: len(tickers) for ac, tickers in snapshot.items()},
        )

    if not snapshot or total_assets == 0:
        log.warn("snapshot empty; nothing to publish")
        return

    with log.pipeline("publish.bind", addr=PUB_ADDR):
        ctx, pub = make_publisher()
        log.info("publisher bound", addr=PUB_ADDR, total_assets=total_assets)

    time.sleep(2.0)  # Wait for subscribers to connect before the first message.

    payload = pickle.dumps(
        {
            "tickers": {
                ac: list(tickers.keys()) for ac, tickers in snapshot.items()
            },
            "start": start.isoformat(),
            "end":   end.isoformat(),
            "data":  snapshot,   # { "equity": {…}, "fx": {…}, "bond": {…} }
        }
    )
    log.info(
        "snapshot serialized",
        bytes=len(payload),
        total_assets=total_assets,
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
                total_assets=total_assets,
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
