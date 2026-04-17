"""--- Import start ---"""

from __future__ import annotations
import pickle
import signal
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import zmq
from _logging import run_module
from config import (
    ASSET_CLASS_EQUITY,
    ASSET_CLASS_FX,
    CYCLE_LOG_EVERY_N,
    DB_URL,
    INITIAL_SUBSCRIBER_GRACE_S,
    INSTRUMENT_UNIVERSES,
    LOOKBACK_DAYS,
    PUB_ADDR,
    TOPIC_OHLCV,
    InstrumentUniverse,
)
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


"""--- ORM models end ---"""


"""--- Nodes start ---

Nodes are the stages of the data module's transformation pipeline. They are
declared in the order the main loop calls them:

    DatabaseNode → FetchStoreNode → SnapshotLoadNode → PublisherNode → ObservabilityNode

Each Node owns its own state and exposes a narrow surface the topology wires
together in ``main``.
"""


class DatabaseNode:
    """Owns the DB engine; rebuilds the schema whenever it drifts from the ORM."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.engine: Engine | None = None

    def setup(self) -> Engine:
        """Create tables if missing or if the schema diverged, and return the engine."""
        with log.pipeline("db.setup", url=self.url):
            engine = create_engine(self.url, future=True)
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
                    url=self.url,
                    error_type=type(e.orig).__name__,
                    error=str(e.orig).splitlines()[0] if str(e.orig) else "",
                    hint="is TimescaleDB running and reachable on that host/port?",
                )
                sys.exit(1)

        self.engine = engine
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


class FetchStoreNode:
    """Pulls OHLCV bars from each universe's adaptor and upserts them into the DB."""

    def __init__(self, engine: Engine, universes: list[InstrumentUniverse]) -> None:
        self.engine = engine
        self.universes = universes
        self._session_factory = sessionmaker(bind=engine, future=True)

    def process(self, start: date, end: date) -> dict[str, int]:
        """Run fetch-and-store across every universe, aggregating per-universe stats."""
        total = {"cached": 0, "fetched": 0, "upserted": 0, "warnings": 0, "errors": 0}
        for universe in self.universes:
            stats = self._fetch_universe(universe, start, end)
            for k, v in stats.items():
                total[k] = total.get(k, 0) + v
        return total

    def _fetch_universe(
        self,
        universe: InstrumentUniverse,
        start: date,
        end: date,
    ) -> dict[str, int]:
        """Fetch OHLCV bars missing between ``[start, end]`` for one universe and upsert them.

        For every symbol we resume from the day after our latest bar in the
        window (or from ``start`` if we have nothing yet). All symbols that
        still need data are fetched in one batch covering the earliest required
        day through ``end``; yfinance is idempotent on overlap and ``merge``
        swallows same-valued rows.
        """
        stats = {"cached": 0, "fetched": 0, "upserted": 0, "warnings": 0, "errors": 0}

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
        self,
        session: Session,
        universe: InstrumentUniverse,
        symbol: str,
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
            detail = EquityDetail(instrument_id=instrument.instrument_id)
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
        """Parse a FX symbol like 'EURUSD=X' into (base_currency, quote_currency)."""
        core = symbol.removesuffix("=X")
        if len(core) != 6:
            raise ValueError(
                f"cannot parse FX symbol {symbol!r}: expected 6-letter base+quote "
                "(yfinance format like 'EURUSD=X')"
            )
        return core[:3], core[3:]


class SnapshotLoadNode:
    """Reads stored OHLCV back out as a ``label → DataFrame`` snapshot."""

    def __init__(self, engine: Engine, universes: list[InstrumentUniverse]) -> None:
        self.engine = engine
        self.universes = universes
        self._session_factory = sessionmaker(bind=engine, future=True)

    def process(self, start: date, end: date) -> dict[str, pd.DataFrame]:
        """Load OHLCV rows for all tickers in the universes and return a snapshot dict."""
        snapshot: dict[str, pd.DataFrame] = {}
        with self._session_factory() as session:
            for universe in self.universes:
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


class PublisherNode:
    """Binds a ZMQ PUB socket and sends each snapshot as a pickled payload."""

    def __init__(self, addr: str, topic: bytes) -> None:
        self.addr = addr
        self.topic = topic
        self.ctx: zmq.Context | None = None
        self.socket: zmq.Socket | None = None

    def bind(self) -> None:
        """Open the PUB socket on ``self.addr``."""
        self.ctx = zmq.Context.instance()
        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.bind(self.addr)

    def publish(
        self,
        snapshot: dict[str, pd.DataFrame],
        start: date,
        end: date,
    ) -> int:
        """Serialize the snapshot and send it on ``self.topic``; return payload size in bytes."""
        payload = pickle.dumps(
            {
                "tickers": list(snapshot.keys()),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "ohlcv": snapshot,
            }
        )
        self.socket.send_multipart([self.topic, payload])
        return len(payload)

    def close(self) -> None:
        """Tear down the socket and ZMQ context with ``linger=0``."""
        if self.socket is not None:
            self.socket.close(linger=0)
        if self.ctx is not None:
            self.ctx.term()


class ObservabilityNode:
    """Folds each cycle's stats into structured snapshots and an adaptive cycle log."""

    def __init__(self, log_every_n: int) -> None:
        self.log_every_n = log_every_n

    def emit_cycle(
        self,
        seq: int,
        start: date,
        end: date,
        stats: dict[str, int],
        snapshot: dict[str, pd.DataFrame],
        payload_bytes: int,
        duration_ms: float,
    ) -> None:
        """Emit the per-cycle snapshot/trace events and a cycle-level log line."""
        emit_data_snapshot(seq, start, end, stats, snapshot, payload_bytes, duration_ms)
        emit_data_trace(seq, start, end, snapshot)
        changed = stats["upserted"] > 0 or stats["warnings"] > 0 or stats["errors"] > 0
        noteworthy = changed or seq == 1 or (seq % self.log_every_n == 0)
        emit = log.info if noteworthy else log.debug
        emit(
            "cycle",
            seq=seq,
            duration_ms=f"{duration_ms:.1f}",
            assets=len(snapshot),
            bytes=payload_bytes,
            **stats,
        )


"""--- Nodes end ---"""


def main() -> None:
    """Declare the data-module pipeline and run the fetch → store → load → publish loop."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    log.info("lookback configured", days=LOOKBACK_DAYS)

    # Topology: DatabaseNode → FetchStoreNode → SnapshotLoadNode → PublisherNode → ObservabilityNode
    db = DatabaseNode(DB_URL)
    engine = db.setup()
    fetch_store = FetchStoreNode(engine, INSTRUMENT_UNIVERSES)
    snapshot_loader = SnapshotLoadNode(engine, INSTRUMENT_UNIVERSES)
    publisher = PublisherNode(PUB_ADDR, TOPIC_OHLCV)
    observer = ObservabilityNode(CYCLE_LOG_EVERY_N)

    # Bootstrap pass: populate DB and confirm we have a non-empty snapshot before binding.
    with log.pipeline("bootstrap", universes=len(INSTRUMENT_UNIVERSES)):
        end = date.today()
        start = end - timedelta(days=LOOKBACK_DAYS)
        log.info("window", start=start.isoformat(), end=end.isoformat())

        stats = fetch_store.process(start, end)
        snapshot = snapshot_loader.process(start, end)
        log.info("bootstrap fetch", assets=len(snapshot), **stats)

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
        publisher.bind()
    time.sleep(INITIAL_SUBSCRIBER_GRACE_S)

    seq = 0
    try:
        while True:
            seq += 1
            t0 = time.monotonic()
            end = date.today()
            start = end - timedelta(days=LOOKBACK_DAYS)

            stats = fetch_store.process(start, end)
            snapshot = snapshot_loader.process(start, end)
            if not snapshot:
                log.warn("cycle snapshot empty", seq=seq)
                continue

            payload_bytes = publisher.publish(snapshot, start, end)
            dur_ms = (time.monotonic() - t0) * 1000.0
            observer.emit_cycle(
                seq, start, end, stats, snapshot, payload_bytes, dur_ms
            )
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", published=seq)
        publisher.close()


if __name__ == "__main__":
    run_module("data", main)
