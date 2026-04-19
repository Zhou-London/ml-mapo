"""Data node catalog for the unified ML-MAPO graph.

Layout:

    DateRange ──┐
                ├─► USEquity ──┐
    Database ───┤               ├─► Aggregate ──► (frame) downstream
                └─► UKEquity ──┘

Each asset-class node pulls OHLCV for its own ticker universe, caching rows
in TimescaleDB and fetching the gap from yfinance when the DB is cold. The
output is a wide ``adj_close`` frame (index = date, columns = tickers).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from adaptors import YfAdaptor
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
    func,
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


# ---------- Helpers ----------


def _split_csv(value: str) -> list[str]:
    return [s.strip() for s in str(value).split(",") if s.strip()]


def _ensure_instrument(
    session: Session, asset_class: str, market: str, symbol: str
) -> int:
    existing = session.execute(
        select(Instrument).where(
            and_(
                Instrument.asset_class == asset_class,
                Instrument.market == market,
                Instrument.symbol == symbol,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing.instrument_id
    inst = Instrument(asset_class=asset_class, market=market, symbol=symbol)
    session.add(inst)
    session.flush()
    return inst.instrument_id


def _fetch_or_load(
    engine: Engine,
    asset_class: str,
    market: str,
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """Return a wide adj_close frame for ``tickers`` in ``[start, end]``.

    Missing bars are fetched from yfinance and upserted into the DB so that
    subsequent ticks for the same window hit the cache.
    """
    if not tickers:
        return pd.DataFrame()

    session_factory = sessionmaker(bind=engine, future=True)
    with session_factory() as session:
        ids = {s: _ensure_instrument(session, asset_class, market, s) for s in tickers}
        session.commit()

        last_ts = dict(
            session.execute(
                select(OHLCV.instrument_id, func.max(OHLCV.ts))
                .where(
                    and_(
                        OHLCV.instrument_id.in_(ids.values()),
                        OHLCV.ts <= end,
                    )
                )
                .group_by(OHLCV.instrument_id)
            ).all()
        )

        to_fetch = [s for s in tickers if (last_ts.get(ids[s]) or date.min) < end]
        if to_fetch:
            fetched = YfAdaptor().fetch_ohlcv_many(to_fetch, start, end)
            for symbol in to_fetch:
                df = fetched.get(symbol)
                if df is None or df.empty:
                    continue
                iid = ids[symbol]
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
            session.commit()

        rows = session.execute(
            select(Instrument.symbol, OHLCV.ts, OHLCV.adj_close)
            .join(OHLCV, OHLCV.instrument_id == Instrument.instrument_id)
            .where(
                and_(
                    Instrument.instrument_id.in_(ids.values()),
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


# ---------- Nodes ----------


@register_node("data/DateRange")
class DateRangeNode(Node):
    """Emits a ``[start, end]`` date window each tick."""

    CATEGORY = "data"
    OUTPUTS = {"start": "date", "end": "date"}
    PARAMS = {
        "start_date": ("str", ""),  # ISO; blank → end - lookback_days
        "end_date": ("str", ""),    # ISO; blank → today
        "lookback_days": ("int", 365),
    }

    def process(self) -> dict:
        end_str = str(self.params["end_date"]).strip()
        start_str = str(self.params["start_date"]).strip()
        end = date.fromisoformat(end_str) if end_str else date.today()
        if start_str:
            start = date.fromisoformat(start_str)
        else:
            start = end - timedelta(days=int(self.params["lookback_days"]))
        return {"start": start, "end": end}


@register_node("data/Database")
class DatabaseNode(Node):
    """Builds the SQLAlchemy engine and ensures the schema + hypertable exist."""

    CATEGORY = "data"
    OUTPUTS = {"engine": "Engine"}
    PARAMS = {"url": ("str", DB_URL)}

    def setup(self) -> None:
        self._engine = create_engine(self.params["url"], future=True)
        self._reset_if_stale(self._engine)
        Base.metadata.create_all(self._engine)
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "SELECT create_hypertable('ohlcv', 'ts', "
                    "if_not_exists => TRUE, migrate_data => TRUE);"
                )
            )

    def process(self) -> dict:
        return {"engine": self._engine}

    @staticmethod
    def _reset_if_stale(engine: Engine) -> None:
        """Drop every ORM-managed table if the on-disk schema doesn't match.

        Keeps the refactor simple: we don't try to migrate old columns, we just
        rebuild the schema. Only touches tables this module owns.
        """
        inspector = inspect(engine)
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
        # Also reset if old subclass tables from earlier schema still exist.
        legacy = {"equity_details", "fx_details"} & existing
        if stale or legacy:
            with engine.begin() as conn:
                for name in list(legacy) + [t.name for t in reversed(Base.metadata.sorted_tables)]:
                    conn.execute(text(f"DROP TABLE IF EXISTS {name} CASCADE;"))


class _AssetNode(Node):
    """Base class shared by the per-asset-class nodes."""

    CATEGORY = "data"
    INPUTS = {"engine": "Engine", "start": "date", "end": "date"}
    OUTPUTS = {"frame": "frame"}
    ASSET_CLASS: str = ""
    MARKET: str = ""

    def process(self, engine: Engine, start: date, end: date) -> dict:
        tickers = _split_csv(self.params["tickers"])
        frame = _fetch_or_load(engine, self.ASSET_CLASS, self.MARKET, tickers, start, end)
        return {"frame": frame}


@register_node("data/USEquity")
class USEquityNode(_AssetNode):
    """US-listed equities via yfinance (e.g. NVDA, AAPL)."""

    ASSET_CLASS = "EQUITY"
    MARKET = "US"
    PARAMS = {"tickers": ("str", US_EQUITIES_DEFAULT)}


@register_node("data/UKEquity")
class UKEquityNode(_AssetNode):
    """LSE-listed equities via yfinance (suffix ``.L``)."""

    ASSET_CLASS = "EQUITY"
    MARKET = "UK"
    PARAMS = {"tickers": ("str", UK_EQUITIES_DEFAULT)}


@register_node("data/FX")
class FXNode(_AssetNode):
    """FX pairs via yfinance (suffix ``=X``)."""

    ASSET_CLASS = "FX"
    MARKET = "FX"
    PARAMS = {"tickers": ("str", FX_PAIRS_DEFAULT)}


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
