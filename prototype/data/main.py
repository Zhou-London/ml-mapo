"""Data module.

Fetches the past 12 months of OHLCV data from yfinance (if missing), stores
it in TimescaleDB via SQLAlchemy ORM, and publishes the snapshot over a
ZeroMQ PUB socket so the risk and forecast modules can consume it.
"""

from __future__ import annotations

import pickle
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

TARGET_TICKERS = ["AAPL", "GOOG", "AMZN", "MSFT", "TSLA", "META", "NVDA"]
LOOKBACK_DAYS = 365
PUBLISH_INTERVAL_S = 5.0


class Base(DeclarativeBase):
    pass


class OHLCV(Base):
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
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] for c in df.columns]
    return df


def fetch_and_store(engine, tickers: list[str], start: date, end: date) -> None:
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        for ticker in tickers:
            count = session.execute(
                select(func.count())
                .select_from(OHLCV)
                .where(and_(OHLCV.ticker == ticker, OHLCV.ts >= start, OHLCV.ts <= end))
            ).scalar_one()
            # ~252 US trading days per year; treat >200 as "cached".
            if count > 200:
                print(f"[data] {ticker}: cached ({count} rows), skip fetch")
                continue

            print(f"[data] fetching {ticker} from yfinance...")
            df = yf.download(
                ticker,
                start=start.isoformat(),
                end=end.isoformat(),
                auto_adjust=False,
                progress=False,
            )
            if df is None or df.empty:
                print(f"[data] {ticker}: no data returned, skipping")
                continue
            df = _flatten_columns(df)

            rows: list[OHLCV] = []
            for ts, row in df.iterrows():
                row_date = ts.date() if hasattr(ts, "date") else ts
                rows.append(
                    OHLCV(
                        ticker=ticker,
                        ts=row_date,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        adj_close=float(row["Adj Close"]),
                        volume=int(row["Volume"]),
                    )
                )
            for r in rows:
                session.merge(r)
            session.commit()
            print(f"[data] {ticker}: stored {len(rows)} rows")


def load_snapshot(engine, tickers: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    Session = sessionmaker(bind=engine, future=True)
    snapshot: dict[str, pd.DataFrame] = {}
    with Session() as session:
        for ticker in tickers:
            rows = (
                session.execute(
                    select(OHLCV)
                    .where(and_(OHLCV.ticker == ticker, OHLCV.ts >= start, OHLCV.ts <= end))
                    .order_by(OHLCV.ts)
                )
                .scalars()
                .all()
            )
            if not rows:
                continue
            frame = pd.DataFrame(
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
            snapshot[ticker] = frame
    return snapshot


def make_publisher() -> tuple[zmq.Context, zmq.Socket]:
    ctx = zmq.Context.instance()
    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)
    return ctx, pub


def main() -> None:
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

    # Give subscribers a moment to finish connecting before the first send.
    time.sleep(2.0)

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
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
