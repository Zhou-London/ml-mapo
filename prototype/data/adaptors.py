"""--- Import start ---"""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

"""--- Import end ---"""

"""--- Config start ---"""
# Canonical column order; every adaptor must return these columns in this
# order. The names mirror yfinance so existing call sites stay unchanged.
OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
DEFAULT_MAX_WORKERS = 16
"""--- Config end ---"""


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OHLCV_COLUMNS)


class DataSourceAdaptor(ABC):
    """Interface for every adaptor.

    Implementations must provide the single-ticker ``fetch_ohlcv``.
    ``fetch_ohlcv_many`` has a default thread-pool implementation so every
    adaptor gets parallelism for free; adaptors whose upstream supports a
    native batch endpoint (e.g. yfinance) should override it.
    """

    name: str = "data_source"
    max_workers: int = DEFAULT_MAX_WORKERS

    @abstractmethod
    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Return [start, end] inclusive OHLCV bars for ticker as a DataFrame with OHLCV_COLUMNS, or an empty frame."""

    def fetch_ohlcv_many(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> dict[str, pd.DataFrame]:
        """Fetch many tickers concurrently. Returns ``{ticker: dataframe}``.

        Default: fan out to ``fetch_ohlcv`` over a thread pool. Missing /
        errored tickers come back as empty frames so the caller can treat
        every request uniformly.
        """
        if not tickers:
            return {}
        out: dict[str, pd.DataFrame] = {}
        workers = min(self.max_workers, len(tickers))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {
                ex.submit(self.fetch_ohlcv, t, start, end): t for t in tickers
            }
            for fut in futs:
                t = futs[fut]
                try:
                    out[t] = fut.result()
                except Exception:
                    out[t] = _empty_frame()
        return out


class YfAdaptor(DataSourceAdaptor):
    """Adapt to yfinance. Uses native batch download for ``fetch_ohlcv_many``."""

    name = "yfinance"

    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        df = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            return _empty_frame()
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = [c[0] for c in df.columns]
        return df[OHLCV_COLUMNS].dropna(how="all")

    def fetch_ohlcv_many(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> dict[str, pd.DataFrame]:
        """Single yfinance batch call. yfinance downloads tickers in parallel
        internally with ``threads=True``, so one call fetches everything at
        once instead of serializing round-trips."""
        if not tickers:
            return {}
        if len(tickers) == 1:
            return {tickers[0]: self.fetch_ohlcv(tickers[0], start, end)}

        df = yf.download(
            tickers,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            auto_adjust=False,
            progress=False,
            group_by="ticker",
            threads=True,
        )
        out: dict[str, pd.DataFrame] = {}
        if df is None or df.empty:
            return {t: _empty_frame() for t in tickers}

        # With group_by='ticker' and multiple tickers, columns are a
        # MultiIndex of (ticker, field). Some yfinance versions return the
        # fields at level 0 instead — handle both.
        if isinstance(df.columns, pd.MultiIndex):
            level0 = set(df.columns.get_level_values(0))
            ticker_at_level_0 = any(t in level0 for t in tickers)
            for t in tickers:
                try:
                    if ticker_at_level_0:
                        sub = df[t]
                    else:
                        sub = df.xs(t, axis=1, level=1, drop_level=True)
                except (KeyError, ValueError):
                    out[t] = _empty_frame()
                    continue
                try:
                    sub = sub[OHLCV_COLUMNS]
                except KeyError:
                    out[t] = _empty_frame()
                    continue
                out[t] = sub.dropna(how="all")
        else:
            # Single-ticker shape despite a list input — rare, but handle it.
            for t in tickers:
                out[t] = df[OHLCV_COLUMNS].dropna(how="all") if t == tickers[0] else _empty_frame()
        return out
