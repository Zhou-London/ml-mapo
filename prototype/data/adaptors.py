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
_OHLCV_COLUMNS: list[str] = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

# MAPIS Fixed Income schema — Section 2.4 of MAPIS_Alpha_README.
# ★ = required by alpha layer; all must be present (NaN allowed for missing values).
# Column names match the reference data layer exactly so alpha factors can
# reference df["yield_to_maturity"] etc. without any translation step.
BOND_COLUMNS: list[str] = [
    "price",                # clean price proxy (ETF close, % of face value)
    "yield_to_maturity",    # ★ YTM — snapshot from .info, broadcast to all rows
    "yield_change",         # ★ daily ΔY (bond equivalent of 'returns')
    "spread_vs_benchmark",  # ★ fund YTM − regional risk-free YTM (value signal)
    "duration",             #   modified duration in years
    "coupon_rate",          #   annual coupon as decimal (fund-level approximation)
    "rolling_vol_21d",      #   21-day std of yield_change (Low-Vol factor + VaR)
]
"""--- Config end ---"""


class DataSourceAdaptor(ABC):
    """Interface for every adaptor"""

    name: str = "data_source"
    asset_class: str = "UNKNOWN"
    schema: list[str] = []

    @abstractmethod
    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Return [start, end] inclusive OHLCV bars for ticker as a DataFrame with OHLCV_COLUMNS, or an empty frame."""


class YfAdaptor(DataSourceAdaptor):
    """Adapt to yfinance"""

    name = "yfinance"
    schema: list[str] = _OHLCV_COLUMNS

    def __init__(self, asset_class: str = "EQUITY") -> None:
        if asset_class not in ("EQUITY", "FX"):
            raise ValueError(
                f"YfAdaptor only supports 'EQUITY' or 'FX', got {asset_class!r}"
            )
        self.asset_class = asset_class

    def fetch_data(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Download OHLCV bars from yfinance.  Identical to the old fetch_ohlcv."""
        df = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
            auto_adjust=False,
            progress=False,
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=_OHLCV_COLUMNS)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = [c[0] for c in df.columns]
        return df[_OHLCV_COLUMNS]

    # Backward-compat shim — remove once all call-sites use fetch_data()
    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Deprecated — delegates to fetch_data()."""
        return self.fetch_data(ticker, start, end)


# ---------------------------------------------------------------------------
# Bond adaptor  (abstract base — concrete implementation in bond_adaptors.py)
# ---------------------------------------------------------------------------


class BondAdaptor(DataSourceAdaptor):
    """Abstract base adaptor for fixed-income instruments.

    Concrete subclass: ``YfBondAdaptor`` in ``bond_adaptors.py``.

    Output schema: BOND_COLUMNS (MAPIS Fixed Income schema, Section 2.4)
        price                – ETF close price (clean price proxy)
        yield_to_maturity ★  – snapshot YTM from Yahoo .info, broadcast
        yield_change      ★  – daily ΔYTM (approximated via price + duration)
        spread_vs_benchmark ★ – fund YTM minus regional risk-free YTM
        duration             – modified duration in years
        coupon_rate          – annual coupon rate, decimal
        rolling_vol_21d      – 21-day std of yield_change

    Index: DatetimeIndex at business-day frequency.
    """

    name: str = "bond"
    asset_class: str = "BOND"
    schema: list[str] = BOND_COLUMNS

    @abstractmethod
    def fetch_data(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Return MAPIS bond time-series for *ticker* over [start, end].

        Return ``pd.DataFrame(columns=BOND_COLUMNS)`` on failure.
        """


# ---------------------------------------------------------------------------
# Schema validation utilities
# ---------------------------------------------------------------------------


def validate_schema(df: pd.DataFrame, adaptor: DataSourceAdaptor) -> pd.DataFrame:
    """Assert every schema column is present in *df* (values may be NaN)."""
    missing = [c for c in adaptor.schema if c not in df.columns]
    if missing:
        raise ValueError(
            f"{adaptor.name!r} ({adaptor.asset_class}): "
            f"DataFrame missing required columns {missing}"
        )
    return df


def empty_frame(adaptor: DataSourceAdaptor) -> pd.DataFrame:
    """Return a zero-row DataFrame matching *adaptor*'s schema."""
    return pd.DataFrame(columns=adaptor.schema)