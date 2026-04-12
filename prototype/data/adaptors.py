"""--- Import start ---"""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

"""--- Import end ---"""

"""--- Config start ---"""
# Columns every adaptor must return, in this order. Missing columns get filled with NaN.
OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
DEFAULT_MAX_WORKERS = 16
"""--- Config end ---"""


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OHLCV_COLUMNS)


class DataSourceAdaptor(ABC):
    """Abstract base class for data source adaptors."""

    name: str = "data_source"
    max_workers: int = DEFAULT_MAX_WORKERS

    @abstractmethod
    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Fetch OHLCV data for a single ticker and date range. Must be implemented by
        subclasses."""
        pass

    def fetch_ohlcv_many(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> dict[str, pd.DataFrame]:
        """Fetch OHLCV data for multiple tickers in parallel. Can be overridden by
        subclasses for optimized batch fetching."""
        if not tickers:
            return {}

        out: dict[str, pd.DataFrame] = {}
        workers = min(self.max_workers, len(tickers))

        with ThreadPoolExecutor(max_workers=workers) as ex:
            # Schedule all fetches in parallel and map futures back to tickers for result collection.
            futs = {ex.submit(self.fetch_ohlcv, t, start, end): t for t in tickers}

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
        """Single-ticker yfinance download."""

        df = yf.download(
            ticker,
            start=start.isoformat(),
            end=(
                end + timedelta(days=1)
            ).isoformat(),  # yfinance returns [start, end) so add one day to end
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            return _empty_frame()

        # Handle both single-level and multi-level column formats. In the multi-level case, we take the first level as the column names.
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
        """Batch yfinance download."""
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

        if isinstance(df.columns, pd.MultiIndex):
            level0 = set(df.columns.get_level_values(0))
            ticker_at_level_0 = any(t in level0 for t in tickers)

            for t in tickers:
                try:
                    # Check which level is ticker located at
                    if ticker_at_level_0:
                        sub = df[t]
                    else:
                        sub = df.xs(t, axis=1, level=1, drop_level=True)
                except (KeyError, ValueError):
                    out[t] = _empty_frame()
                    continue

                try:
                    # Extract required columns
                    sub = sub[OHLCV_COLUMNS]
                except KeyError:
                    out[t] = _empty_frame()
                    continue

                # Drop rows with empty OHLCV
                out[t] = sub.dropna(how="all")
        else:
            for t in tickers:
                out[t] = (
                    df[OHLCV_COLUMNS].dropna(how="all")
                    if t == tickers[0]
                    else _empty_frame()
                )
        return out
