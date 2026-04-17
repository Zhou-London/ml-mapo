"""Data node catalog for the unified ML-MAPO graph."""

from __future__ import annotations

import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from config import (
    ASSET_CLASS_EQUITY,
    ASSET_CLASS_FX,
    CYCLE_LOG_EVERY_N,
    DB_URL,
    INSTRUMENT_UNIVERSES,
    LOOKBACK_DAYS,
    InstrumentUniverse,
)
from graph import Node, register_node
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


"""--- ORM models start ---"""


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
    __tablename__ = "equity_details"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
        primary_key=True,
    )


class FxDetail(Base):
    __tablename__ = "fx_details"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
        primary_key=True,
    )
    base_currency: Mapped[str] = mapped_column(String(8), nullable=False)
    quote_currency: Mapped[str] = mapped_column(String(8), nullable=False)


class OHLCV(Base):
    """Daily OHLCV bar keyed by (instrument_id, ts). ``ts`` is the hypertable partition column."""

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


"""--- ORM models end ---"""


"""--- Graph nodes start ---"""


@register_node("data/Clock")
class ClockNode(Node):
    """Emits the rolling ``[start, end]`` window each tick."""

    CATEGORY = "data"
    OUTPUTS = {"start": "date", "end": "date"}
    PARAMS = {"lookback_days": ("int", LOOKBACK_DAYS)}

    def process(self) -> dict:
        end = date.today()
        start = end - timedelta(days=int(self.params["lookback_days"]))
        return {"start": start, "end": end}


@register_node("data/Database")
class DatabaseNode(Node):
    """Creates the SQLAlchemy engine and ensures the schema + hypertable exist."""

    CATEGORY = "data"
    OUTPUTS = {"engine": "Engine"}
    PARAMS = {"url": ("str", DB_URL)}

    def setup(self) -> None:
        self._engine = self._build_engine()

    def process(self) -> dict:
        return {"engine": self._engine}

    def _build_engine(self) -> Engine:
        url = self.params["url"]
        with log.pipeline("db.setup", url=url):
            engine = create_engine(url, future=True)
            try:
                if not self._schema_matches(engine):
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
                    log.info("schema ok", tables=len(Base.metadata.sorted_tables))

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
                    url=url,
                    error_type=type(e.orig).__name__,
                    error=str(e.orig).splitlines()[0] if str(e.orig) else "",
                    hint="is TimescaleDB running and reachable on that host/port?",
                )
                sys.exit(1)
        return engine

    @staticmethod
    def _schema_matches(engine: Engine) -> bool:
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


@register_node("data/FetchStore")
class FetchStoreNode(Node):
    """Pulls OHLCV bars from each universe's adaptor and upserts them into the DB."""

    CATEGORY = "data"
    INPUTS = {"engine": "Engine", "start": "date", "end": "date"}
    OUTPUTS = {"stats": "dict"}

    def setup(self) -> None:
        self._session_factory: sessionmaker | None = None
        self._universes: list[InstrumentUniverse] = list(INSTRUMENT_UNIVERSES)

    def process(self, engine: Engine, start: date, end: date) -> dict:
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=engine, future=True)
        total = {"cached": 0, "fetched": 0, "upserted": 0, "warnings": 0, "errors": 0}
        for universe in self._universes:
            stats = self._fetch_universe(universe, start, end)
            for k, v in stats.items():
                total[k] = total.get(k, 0) + v
        return {"stats": total}

    def _fetch_universe(
        self, universe: InstrumentUniverse, start: date, end: date
    ) -> dict[str, int]:
        stats = {"cached": 0, "fetched": 0, "upserted": 0, "warnings": 0, "errors": 0}
        assert self._session_factory is not None

        with self._session_factory() as session:
            ids = {
                s: self._ensure_instrument(session, universe, s)
                for s in universe.tickers
            }
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

    def _ensure_instrument(
        self, session: Session, universe: InstrumentUniverse, symbol: str
    ) -> int:
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

        currency: str | None = None
        if universe.asset_class == ASSET_CLASS_FX:
            _, currency = self._parse_fx_symbol(symbol)

        instrument = Instrument(
            asset_class=universe.asset_class,
            market=universe.market,
            symbol=symbol,
            currency=currency,
        )
        session.add(instrument)
        session.flush()

        if universe.asset_class == ASSET_CLASS_EQUITY:
            detail: EquityDetail | FxDetail = EquityDetail(
                instrument_id=instrument.instrument_id
            )
        elif universe.asset_class == ASSET_CLASS_FX:
            base, quote = self._parse_fx_symbol(symbol)
            detail = FxDetail(
                instrument_id=instrument.instrument_id,
                base_currency=base,
                quote_currency=quote,
            )
        else:
            raise ValueError(f"unknown asset_class: {universe.asset_class!r}")
        session.add(detail)
        return instrument.instrument_id

    @staticmethod
    def _parse_fx_symbol(symbol: str) -> tuple[str, str]:
        core = symbol.removesuffix("=X")
        if len(core) != 6:
            raise ValueError(
                f"cannot parse FX symbol {symbol!r}: expected 6-letter base+quote "
                "(yfinance format like 'EURUSD=X')"
            )
        return core[:3], core[3:]


@register_node("data/SnapshotLoad")
class SnapshotLoadNode(Node):
    """Reads stored OHLCV back out as a ``label → DataFrame`` snapshot."""

    CATEGORY = "data"
    INPUTS = {"engine": "Engine", "start": "date", "end": "date"}
    OUTPUTS = {"snapshot": "ohlcv_snapshot"}

    def setup(self) -> None:
        self._session_factory: sessionmaker | None = None
        self._universes: list[InstrumentUniverse] = list(INSTRUMENT_UNIVERSES)

    def process(self, engine: Engine, start: date, end: date) -> dict:
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=engine, future=True)
        snapshot: dict[str, pd.DataFrame] = {}
        with self._session_factory() as session:
            for universe in self._universes:
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
        return {"snapshot": snapshot}


@register_node("data/Observer")
class ObserverNode(Node):
    """Folds each tick's stats into structured snapshot/trace events + cycle log."""

    CATEGORY = "data"
    INPUTS = {
        "stats": "dict",
        "snapshot": "ohlcv_snapshot",
        "start": "date",
        "end": "date",
    }
    PARAMS = {"log_every_n": ("int", CYCLE_LOG_EVERY_N)}

    def process(
        self,
        stats: dict,
        snapshot: dict,
        start: date,
        end: date,
    ) -> dict:
        seq = int(self.ctx.get("seq", 0))
        t0 = self.ctx.get("t0", time.monotonic())
        duration_ms = (time.monotonic() - t0) * 1000.0
        payload_bytes = 0

        emit_data_snapshot(seq, start, end, stats, snapshot, payload_bytes, duration_ms)
        emit_data_trace(seq, start, end, snapshot)

        changed = stats["upserted"] > 0 or stats["warnings"] > 0 or stats["errors"] > 0
        every_n = int(self.params["log_every_n"]) or 1
        noteworthy = changed or seq == 1 or (seq % every_n == 0)
        emit = log.info if noteworthy else log.debug
        emit(
            "cycle",
            seq=seq,
            duration_ms=f"{duration_ms:.1f}",
            assets=len(snapshot),
            bytes=payload_bytes,
            **stats,
        )
        return {}
