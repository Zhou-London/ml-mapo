"""Data node catalog for the unified ML-MAPO graph.

Layout:

    DateRange ──┐
                ├─► USEquity ──┐
    Database ───┤               ├─► Aggregate ──► (frame) downstream
                └─► UKEquity ──┘

``Database`` exposes a ``DataStore`` handle — DB persistence only.
Each asset-class node picks its own adaptor and implements ``fetch(...)``,
i.e. how to use that adaptor. The store resolves gaps by calling the
asset node's fetch callback.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from adaptors import DataSourceAdaptor, YfAdaptor
from graph import Node, register_node
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
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


DB_URL = "postgresql+psycopg2://postgres:password@localhost:6543/postgres"

US_EQUITIES_DEFAULT = "NVDA,AAPL,MSFT,AMZN,GOOGL,META,TSLA,AVGO,COST,NFLX"
UK_EQUITIES_DEFAULT = "HSBA.L,BP.L,SHEL.L,AZN.L,ULVR.L"
FX_PAIRS_DEFAULT = "EURUSD=X,GBPUSD=X,AUDUSD=X,JPYUSD=X"


# ---------- ORM ----------


class Base(DeclarativeBase):
    pass


class Instrument(Base):
    __tablename__ = "instruments"

    instrument_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_class: Mapped[str] = mapped_column(String(16), nullable=False)
    market: Mapped[str] = mapped_column(String(16), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    # Tracks *what we asked the adaptor for* (not what came back) so weekend /
    # holiday gaps don't trigger a refetch on every tick.
    fetched_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    fetched_end: Mapped[date | None] = mapped_column(Date, nullable=True)

    __table_args__ = (
        UniqueConstraint("asset_class", "market", "symbol", name="uq_instruments_key"),
    )


class OHLCV(Base):
    __tablename__ = "ohlcv"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.instrument_id", ondelete="CASCADE"), primary_key=True
    )
    ts: Mapped[date] = mapped_column(Date, primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    adj_close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)


# ---------- DataStore ----------


def _split_csv(value: str) -> list[str]:
    return [s.strip() for s in str(value).split(",") if s.strip()]


class DataStore:
    """DB persistence helpers for asset-class nodes.

    Two public primitives:

    - ``fetch_gaps(asset_class, market, tickers, start, end, adaptor)``
      ensures the DB covers ``[start, end]`` using the supplied adaptor.
    - ``read_frame(asset_class, market, tickers, start, end)`` returns a
      wide ``adj_close`` frame from the DB.

    Asset nodes glue these together in their own ``process`` — they pick
    whichever adaptor they want.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._Session = sessionmaker(bind=engine, future=True)

    def initialize(self) -> None:
        """Reset stale tables, create schema, promote ``ohlcv`` to a hypertable."""
        self._reset_if_stale()
        Base.metadata.create_all(self.engine)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "SELECT create_hypertable('ohlcv', 'ts', "
                    "if_not_exists => TRUE, migrate_data => TRUE);"
                )
            )

    def fetch_gaps(
        self,
        asset_class: str,
        market: str,
        tickers: list[str],
        start: date,
        end: date,
        adaptor: DataSourceAdaptor,
    ) -> None:
        """Ask ``adaptor`` for any bars in ``[start, end]`` we don't have yet."""
        if not tickers:
            return
        with self._Session() as session:
            instruments = self._ensure_instruments(session, asset_class, market, tickers)
            session.commit()
            gaps: dict[tuple[date, date], list[str]] = defaultdict(list)
            for symbol, inst in instruments.items():
                fs, fe = inst.fetched_start, inst.fetched_end
                if fs is None or fe is None:
                    gaps[(start, end)].append(symbol)
                    continue
                if start < fs:
                    gaps[(start, fs - timedelta(days=1))].append(symbol)
                if end > fe:
                    gaps[(fe + timedelta(days=1), end)].append(symbol)
            if not gaps:
                return
            for (gstart, gend), syms in gaps.items():
                fetched = adaptor.fetch_ohlcv_many(syms, gstart, gend)
                for symbol in syms:
                    inst = instruments[symbol]
                    inst.fetched_start = (
                        gstart if inst.fetched_start is None
                        else min(inst.fetched_start, gstart)
                    )
                    inst.fetched_end = (
                        gend if inst.fetched_end is None
                        else max(inst.fetched_end, gend)
                    )
                    df = fetched.get(symbol)
                    if df is None or df.empty:
                        continue
                    for ts, row in df.iterrows():
                        session.merge(
                            OHLCV(
                                instrument_id=inst.instrument_id,
                                ts=ts.date() if hasattr(ts, "date") else ts,
                                open=float(row["Open"]),
                                high=float(row["High"]),
                                low=float(row["Low"]),
                                close=float(row["Close"]),
                                adj_close=float(row["Adj Close"]),
                                volume=int(row["Volume"]),
                            )
                        )
            session.commit()

    def read_frame(
        self,
        asset_class: str,
        market: str,
        tickers: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Wide ``adj_close`` frame for ``tickers`` from the DB."""
        if not tickers:
            return pd.DataFrame()
        with self._Session() as session:
            instruments = self._ensure_instruments(session, asset_class, market, tickers)
            session.commit()
            ids = [inst.instrument_id for inst in instruments.values()]
            rows = session.execute(
                select(Instrument.symbol, OHLCV.ts, OHLCV.adj_close)
                .join(OHLCV, OHLCV.instrument_id == Instrument.instrument_id)
                .where(
                    and_(
                        Instrument.instrument_id.in_(ids),
                        OHLCV.ts >= start,
                        OHLCV.ts <= end,
                    )
                )
                .order_by(OHLCV.ts, Instrument.symbol)
            ).all()
        if not rows:
            return pd.DataFrame()
        long = pd.DataFrame(rows, columns=["symbol", "ts", "adj_close"])
        return long.pivot(index="ts", columns="symbol", values="adj_close").sort_index()

    # ---------- internals ----------

    def _ensure_instruments(
        self, session: Session, asset_class: str, market: str, tickers: list[str]
    ) -> dict[str, Instrument]:
        out: dict[str, Instrument] = {}
        for symbol in tickers:
            existing = session.execute(
                select(Instrument).where(
                    and_(
                        Instrument.asset_class == asset_class,
                        Instrument.market == market,
                        Instrument.symbol == symbol,
                    )
                )
            ).scalar_one_or_none()
            if existing is None:
                existing = Instrument(asset_class=asset_class, market=market, symbol=symbol)
                session.add(existing)
                session.flush()
            out[symbol] = existing
        return out

    def _reset_if_stale(self) -> None:
        """Drop ORM-managed tables if the on-disk schema doesn't match."""
        inspector = inspect(self.engine)
        existing = set(inspector.get_table_names())
        stale = False
        for table in Base.metadata.sorted_tables:
            if table.name not in existing:
                continue
            expected = {c.name for c in table.columns}
            actual = {c["name"] for c in inspector.get_columns(table.name)}
            if expected != actual:
                stale = True
                break
        legacy = {"equity_details", "fx_details"} & existing
        if stale or legacy:
            with self.engine.begin() as conn:
                for name in list(legacy) + [
                    t.name for t in reversed(Base.metadata.sorted_tables)
                ]:
                    conn.execute(text(f"DROP TABLE IF EXISTS {name} CASCADE;"))


# ---------- Nodes ----------


@register_node("data/DateRange")
class DateRangeNode(Node):
    """Emits a ``[start, end]`` date window each tick."""

    CATEGORY = "data"
    OUTPUTS = {"start": "date", "end": "date"}
    PARAMS = {
        "start_date": ("str", ""),  # ISO; blank → today - 365 days
        "end_date": ("str", ""),    # ISO; blank → today
    }

    def process(self) -> dict:
        end_str = str(self.params["end_date"]).strip()
        start_str = str(self.params["start_date"]).strip()
        end = date.fromisoformat(end_str) if end_str else date.today()
        start = (
            date.fromisoformat(start_str) if start_str else end - timedelta(days=365)
        )
        return {"start": start, "end": end}


@register_node("data/Database")
class DatabaseNode(Node):
    """Builds the DataStore and initializes the schema + hypertable."""

    CATEGORY = "data"
    OUTPUTS = {"store": "store"}
    PARAMS = {"url": ("str", DB_URL)}

    def setup(self) -> None:
        engine = create_engine(self.params["url"], future=True)
        self._store = DataStore(engine)
        self._store.initialize()

    def process(self) -> dict:
        return {"store": self._store}


class _AssetNode(Node):
    """Base for per-asset-class nodes.

    Shared ports + two constants (``ASSET_CLASS``, ``MARKET``). Each
    subclass writes its own ``process`` — pick an adaptor, call
    ``store.fetch_gaps(..., adaptor)``, then ``store.read_frame(...)``.
    Free to skip either step or glue in its own logic.
    """

    CATEGORY = "data"
    INPUTS = {"store": "store", "start": "date", "end": "date"}
    OUTPUTS = {"frame": "frame"}
    ASSET_CLASS: str = ""
    MARKET: str = ""


@register_node("data/USEquity")
class USEquityNode(_AssetNode):
    """US-listed equities via yfinance (e.g. NVDA, AAPL)."""

    ASSET_CLASS = "EQUITY"
    MARKET = "US"
    PARAMS = {"tickers": ("str", US_EQUITIES_DEFAULT)}

    def process(self, store: DataStore, start: date, end: date) -> dict:
        tickers = _split_csv(self.params["tickers"])
        store.fetch_gaps(self.ASSET_CLASS, self.MARKET, tickers, start, end, YfAdaptor())
        return {"frame": store.read_frame(self.ASSET_CLASS, self.MARKET, tickers, start, end)}


@register_node("data/UKEquity")
class UKEquityNode(_AssetNode):
    """LSE-listed equities via yfinance (suffix ``.L``)."""

    ASSET_CLASS = "EQUITY"
    MARKET = "UK"
    PARAMS = {"tickers": ("str", UK_EQUITIES_DEFAULT)}

    def process(self, store: DataStore, start: date, end: date) -> dict:
        tickers = _split_csv(self.params["tickers"])
        store.fetch_gaps(self.ASSET_CLASS, self.MARKET, tickers, start, end, YfAdaptor())
        return {"frame": store.read_frame(self.ASSET_CLASS, self.MARKET, tickers, start, end)}


@register_node("data/FX")
class FXNode(_AssetNode):
    """FX pairs via yfinance (suffix ``=X``)."""

    ASSET_CLASS = "FX"
    MARKET = "FX"
    PARAMS = {"tickers": ("str", FX_PAIRS_DEFAULT)}

    def process(self, store: DataStore, start: date, end: date) -> dict:
        tickers = _split_csv(self.params["tickers"])
        store.fetch_gaps(self.ASSET_CLASS, self.MARKET, tickers, start, end, YfAdaptor())
        return {"frame": store.read_frame(self.ASSET_CLASS, self.MARKET, tickers, start, end)}


@register_node("data/Aggregate")
class AggregateNode(Node):
    """Concatenates two asset-class frames column-wise into one frame.

    Chain multiple Aggregates to combine more than two asset classes.
    """

    CATEGORY = "data"
    INPUTS = {"a": "frame", "b": "frame"}
    OUTPUTS = {"frame": "frame"}

    def process(self, a: pd.DataFrame, b: pd.DataFrame) -> dict:
        combined = pd.concat([a, b], axis=1).sort_index()
        combined = combined.loc[:, ~combined.columns.duplicated()]
        return {"frame": combined}
