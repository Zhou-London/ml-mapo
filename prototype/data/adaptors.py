"""--- Import start ---"""

from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import date, timedelta
import pandas as pd
import yfinance as yf

"""--- Import end ---"""

"""--- Config start ---"""
# Canonical column order; every adaptor must return these columns in this
# order. The names mirror yfinance so existing call sites stay unchanged.
OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
"""--- Config end ---"""


class DataSourceAdaptor(ABC):
    """Interface for every adaptor"""

    name: str = "data_source"

    @abstractmethod
    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Return [start, end] inclusive OHLCV bars for ticker as a DataFrame with OHLCV_COLUMNS, or an empty frame."""


class YfAdaptor(DataSourceAdaptor):
    """Adapt to yfinance"""

    name = "yfinance"

    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        df = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
            auto_adjust=False,
            progress=False,
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = [c[0] for c in df.columns]
        return df[OHLCV_COLUMNS]
