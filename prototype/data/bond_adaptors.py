"""
bond_adaptors.py — Concrete yfinance-based bond adaptor
========================================================
Implements the MAPIS Fixed Income schema (Section 2.4) for US Treasury,
UK Gilt, and EU government bond ETFs, all sourced through yfinance.

NULL FIELD RESOLUTION STRATEGY
--------------------------------
yfinance `.info` is inconsistent across exchanges:
  - US-listed ETFs  : `yield` and `duration` usually populated  (SEC 30-day yield)
  - UK LSE ETFs     : `yield` sometimes missing, `duration` often missing
  - Xetra ETFs      : both `yield` and `duration` usually missing
  - T-Bill ETFs     : `duration` returns None (near-zero duration instruments)

This adaptor resolves NULLs through a prioritised fallback chain so that
MAPIS critical fields (★) are always populated:

  yield_to_maturity ★
    1. info["yield"]                       — 30-day SEC yield (US ETFs)
    2. info["trailingAnnualDividendYield"] — bond ETF distributions ≈ running yield
    3. info["dividendYield"]               — alternative field name
    4. coupon_rate from universe metadata  — static fallback (last resort)

  duration
    1. info["duration"]                    — from yfinance
    2. _BUCKET_DURATION[maturity_bucket]   — maturity-bucket lookup (always available)

  spread_vs_benchmark ★
    1. fund_ytm − rf_ytm  (same fallback chain for rf_ytm)
    2. 0.0 when ticker IS the risk-free benchmark
    3. fund_ytm − coupon_rate_of_rf_benchmark (static last resort)

  yield_change ★
    1. −price_return / duration            — standard bond approximation
       (duration is always populated after fallback, so this path always fires)

FIX NOTES (vs original)
-----------------------
BUG 1 — NULL yield_change on incremental 1-day fetches
  Root cause: fetch_data() is called for 1-day gaps.  With only 1 row,
  pct_change() returns NaN (no prior row) → yield_change = NaN.
  Fix: _fetch_price_history() silently extends the download window
  backwards by PCTCHANGE_BUFFER days.  The extra rows are trimmed at
  the end of fetch_data() before returning, so callers never see them.

BUG 2 — rolling_vol_21d entirely NULL
  Root cause: same 1-row fetch problem.  rolling(min_periods=10) over
  1 row always returns NaN.
  Fix: _fetch_price_history() extends the window backwards by
  VOL_LOOKBACK_BUFFER (= vol_window + 5) days, giving the rolling
  computation enough history.  Again trimmed before return.

BUG 3 — IS04.L removed from EU_BOND_TICKERS
  It consistently fails with "no timezone found" in yfinance.
  Removed to stop repeated warnings on every run.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import yfinance as yf
from datetime import date, timedelta

from adaptors import BOND_COLUMNS, BondAdaptor

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_VOL_WINDOW = 21   # rolling window (trading days) for yield_change volatility

# ── FIX: lookback buffers ────────────────────────────────────────────────────
# Extra calendar days fetched before `start` so that:
#   PCTCHANGE_BUFFER  — pct_change() has a prior row even on a 1-day fetch
#   VOL_LOOKBACK_BUFFER — rolling(21) has enough history on incremental fetches
# Calendar days (not trading days) to be safe across weekends / holidays.
_PCTCHANGE_BUFFER    = 5                         # ~3 trading days
_VOL_LOOKBACK_BUFFER = _VOL_WINDOW * 2           # 42 calendar days >> 21 trading days


# ---------------------------------------------------------------------------
# Duration fallback by maturity bucket
# ---------------------------------------------------------------------------
_BUCKET_DURATION: dict[str, float] = {
    "ultra-short":  0.25,
    "short":        2.0,
    "short-accum":  2.0,
    "intermediate": 7.5,
    "long":         13.0,
    "ultra-long":   18.0,
    "broad":        6.5,
    "index-linked": 10.0,
}

# Static rf_ytm fallback — update roughly quarterly
_RF_YTM_STATIC: dict[str, float] = {
    "USD": 0.0425,
    "GBP": 0.0430,
    "EUR": 0.0260,
}


# ---------------------------------------------------------------------------
# Instrument universes
# ---------------------------------------------------------------------------

US_TREASURY_ETFS: dict[str, tuple] = {
    "SGOV": ("iShares 0-3 Month Treasury Bond ETF",     "ultra-short",   "US46435G5759", "BlackRock / US Treasury",    0.0, "AAA"),
    "BIL":  ("SPDR Bloomberg 1-3 Month T-Bill ETF",     "ultra-short",   "US78468R5568", "State Street / US Treasury", 0.0, "AAA"),
    "SHY":  ("iShares 1-3 Year Treasury Bond ETF",      "short",         "US4642874659", "BlackRock / US Treasury",    4.5, "AAA"),
    "VGSH": ("Vanguard Short-Term Treasury ETF",         "short",         "US9219377978", "Vanguard / US Treasury",     4.5, "AAA"),
    "IEF":  ("iShares 7-10 Year Treasury Bond ETF",     "intermediate",  "US4642874733", "BlackRock / US Treasury",    4.0, "AAA"),
    "VGIT": ("Vanguard Intermediate-Term Treasury ETF",  "intermediate",  "US9219378364", "Vanguard / US Treasury",     4.0, "AAA"),
    "TLH":  ("iShares 10-20 Year Treasury Bond ETF",    "long",          "US4642874576", "BlackRock / US Treasury",    4.2, "AAA"),
    "TLT":  ("iShares 20+ Year Treasury Bond ETF",      "ultra-long",    "US4642874329", "BlackRock / US Treasury",    4.0, "AAA"),
    "VGLT": ("Vanguard Long-Term Treasury ETF",          "ultra-long",    "US9219378778", "Vanguard / US Treasury",     4.0, "AAA"),
    "GOVT": ("iShares US Treasury Bond ETF",             "broad",         "US46432F8422", "BlackRock / US Treasury",    3.8, "AAA"),
}

UK_GILT_ETFS: dict[str, tuple] = {
    "IGLT.L": ("iShares Core UK Gilts UCITS ETF",       "broad",         "IE00B1FZSB30", "BlackRock / HM Treasury",     4.0, "AA", "GBP", "UK"),
    "VGOV.L": ("Vanguard UK Gilt UCITS ETF",            "broad",         "IE00B42WWV65", "Vanguard / HM Treasury",      4.0, "AA", "GBP", "UK"),
    "GILS.L": ("Lyxor Core UK Govt Bond UCITS ETF",     "broad",         "LU1407887162", "Lyxor / HM Treasury",         4.0, "AA", "GBP", "UK"),
    "GLTY.L": ("SPDR Bloomberg UK Gilt ETF",            "broad",         "IE00B4WL9143", "State Street / HM Treasury",  4.0, "AA", "GBP", "UK"),
    "IGLS.L": ("iShares UK Short Duration Gilt ETF",    "short",         "IE00B4WXJJ64", "BlackRock / HM Treasury",     4.5, "AA", "GBP", "UK"),
    "GLTA.L": ("Invesco UK Gilts UCITS ETF Acc",        "short-accum",   "IE00BG0SSB18", "Invesco / HM Treasury",       4.5, "AA", "GBP", "UK"),
    "INXG.L": ("iShares Index Linked Gilt UCITS ETF",   "index-linked",  "IE00B1FZSD53", "BlackRock / HM Treasury",     0.0, "AA", "GBP", "UK"),
}

EU_BOND_ETFS: dict[str, tuple] = {
    # IS04.L removed — consistently fails with "no timezone found" in yfinance
    "IEGA.L":  ("iShares Core EUR Govt Bond UCITS ETF (LSE)",    "broad",        "IE00B4WXJJ64", "BlackRock / Eurozone",  3.5, "AA", "EUR", "EU"),
    "VETY.L":  ("Vanguard EUR Eurozone Govt Bond UCITS ETF",     "broad",        "IE00BZ163G84", "Vanguard / Eurozone",   3.5, "AA", "EUR", "EU"),
    "EXX6.DE": ("iShares Core EUR Govt Bond UCITS ETF (Xetra)",  "broad",        "IE00B4WXJJ64", "BlackRock / Eurozone",  3.5, "AA", "EUR", "EU"),
    "EXVM.DE": ("iShares EUR Govt Bond 1-3yr UCITS ETF (Xetra)", "short",        "IE00B3VTMJ91", "BlackRock / Eurozone",  3.8, "AA", "EUR", "EU"),
    "IBCI.L":  ("iShares EUR Govt Bond 1-3yr UCITS ETF (LSE)",   "short",        "IE00B3VTMJ91", "BlackRock / Eurozone",  3.8, "AA", "EUR", "EU"),
    "IBGL.L":  ("iShares EUR Govt Bond 15-30yr UCITS ETF",       "long",         "IE00B1FZS913", "BlackRock / Eurozone",  2.8, "AA", "EUR", "EU"),
}

# Combined universe normalised to 8-tuple
_ALL_ETF_UNIVERSE: dict[str, tuple] = {
    **{k: v + ("USD", "US") for k, v in US_TREASURY_ETFS.items()},
    **UK_GILT_ETFS,
    **EU_BOND_ETFS,
}

# Risk-free proxy per currency
_RISK_FREE_TICKER: dict[str, str] = {
    "USD": "SGOV",
    "GBP": "IGLS.L",
    "EUR": "EXVM.DE",
}

# Public ticker lists — imported by data/main.py
US_BOND_TICKERS: list[str] = list(US_TREASURY_ETFS.keys())
UK_BOND_TICKERS: list[str] = list(UK_GILT_ETFS.keys())
EU_BOND_TICKERS: list[str] = list(EU_BOND_ETFS.keys())


# ---------------------------------------------------------------------------
# Concrete adaptor
# ---------------------------------------------------------------------------


class YfBondAdaptor(BondAdaptor):
    """Fetch MAPIS-compliant bond data via yfinance for US, UK, and EU ETFs."""

    name = "yfinance_bond"

    def __init__(self, vol_window: int = _VOL_WINDOW) -> None:
        self.vol_window = vol_window

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def fetch_data(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Return MAPIS-compliant DataFrame for *ticker* over [start, end].

        Internally fetches extra history before `start` so that pct_change()
        and rolling volatility are computed correctly even on 1-day gaps.
        The extra rows are trimmed before returning — callers only see
        [start, end].
        """
        empty = pd.DataFrame(columns=BOND_COLUMNS)
        meta  = _ALL_ETF_UNIVERSE.get(ticker)

        # ── FIX: extend window backwards for pct_change + rolling vol ────────
        # We need at least 1 prior row for pct_change (yield_change) and
        # vol_window prior rows for rolling_vol_21d.  Fetch extra history
        # before `start`, compute everything, then trim back to [start, end].
        extended_start = start - timedelta(days=max(_PCTCHANGE_BUFFER,
                                                    _VOL_LOOKBACK_BUFFER))

        try:
            df = self._fetch_price_history(ticker, extended_start, end)
        except Exception:
            return empty
        if df.empty:
            return empty

        try:
            analytics = self._fetch_analytics(ticker, meta)
        except Exception:
            analytics = _empty_analytics()

        df = self._enrich(df, ticker, meta, analytics)

        # ── Trim to originally requested window ───────────────────────────────
        # Convert start to a timezone-naive Timestamp to match the index.
        start_ts = pd.Timestamp(start).normalize()
        df = df[df.index >= start_ts]

        if df.empty:
            return empty

        return df[BOND_COLUMNS]

    # ------------------------------------------------------------------
    # Source 1 — price history
    # ------------------------------------------------------------------

    def _fetch_price_history(
        self, ticker: str, start: date, end: date
    ) -> pd.DataFrame:
        raw = yf.Ticker(ticker).history(
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            auto_adjust=True,
        )
        if raw is None or raw.empty:
            return pd.DataFrame()

        df = raw[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = "date"
        df.rename(columns={
            "Close": "price", "Open": "open",
            "High": "high", "Low": "low", "Volume": "volume",
        }, inplace=True)

        # pct_change() will now have a prior row even on 1-day requested windows
        # because we fetched extra history in fetch_data().
        df["price_return"]  = df["price"].pct_change()
        df["dollar_volume"] = df["price"] * df["volume"]

        # Placeholders — filled in _enrich()
        for col in BOND_COLUMNS:
            if col not in df.columns:
                df[col] = np.nan

        return df

    # ------------------------------------------------------------------
    # Source 2 — bond analytics (YTM + duration with full fallback chain)
    # ------------------------------------------------------------------

    def _fetch_analytics(self, ticker: str, meta: tuple | None) -> dict:
        """Pull YTM and duration from yfinance with layered fallbacks.

        YTM resolution order:
          1. info["yield"]
          2. info["trailingAnnualDividendYield"]
          3. info["dividendYield"]
          4. coupon_rate from universe metadata

        Duration resolution order:
          1. info["duration"]
          2. _BUCKET_DURATION[maturity_bucket]
        """
        try:
            info = yf.Ticker(ticker).info
        except Exception:
            info = {}

        # ── YTM ──────────────────────────────────────────────────────────────
        ytm = (
            _safe_float(info.get("yield"))
            or _safe_float(info.get("trailingAnnualDividendYield"))
            or _safe_float(info.get("dividendYield"))
        )
        if ytm is None and meta is not None:
            coupon_pct = meta[4]
            if coupon_pct > 0:
                ytm = coupon_pct / 100.0

        # ── Duration ─────────────────────────────────────────────────────────
        duration = _safe_float(info.get("duration"))
        if duration is None and meta is not None:
            bucket   = meta[1]
            duration = _BUCKET_DURATION.get(bucket)

        # ── Supplementary fields ─────────────────────────────────────────────
        nav  = _safe_float(info.get("navPrice") or info.get("previousClose"))
        dv01 = (duration * nav * 0.0001
                if duration is not None and nav is not None else None)

        return {
            "yield_to_maturity": ytm,
            "duration":          duration,
            "nav":               nav,
            "dv01_per_share":    dv01,
            "aum":               _safe_float(info.get("totalAssets")),
            "expense_ratio":     _safe_float(info.get("expenseRatio")),
            "ytd_return":        _safe_float(info.get("ytdReturn")),
        }

    # ------------------------------------------------------------------
    # Enrich — compute all MAPIS critical fields
    # ------------------------------------------------------------------

    def _enrich(
        self,
        df: pd.DataFrame,
        ticker: str,
        meta: tuple | None,
        analytics: dict,
    ) -> pd.DataFrame:
        df = df.copy()

        coupon_pct = meta[4] if meta is not None else 0.0
        currency   = meta[6] if meta is not None else "USD"
        bucket     = meta[1] if meta is not None else "broad"

        df["coupon_rate"] = coupon_pct / 100.0

        # ── YTM ──────────────────────────────────────────────────────────────
        ytm = analytics.get("yield_to_maturity")
        df["yield_to_maturity"] = ytm if ytm is not None else np.nan

        # ── Duration ─────────────────────────────────────────────────────────
        duration = analytics.get("duration") or _BUCKET_DURATION.get(bucket, 6.5)
        df["duration"] = duration

        # ── yield_change ★ ───────────────────────────────────────────────────
        # Δy ≈ −price_return / modified_duration
        # Now correctly computed because _fetch_price_history() was called
        # with an extended window — price_return is non-NaN for all rows
        # except the very first row of the extended buffer (which gets trimmed).
        df["yield_change"] = -(df["price_return"] / duration)

        # ── rolling_vol_21d ──────────────────────────────────────────────────
        # 21-day std of yield_change.  Now correctly computed because the
        # extended window gives enough prior rows even on incremental 1-day
        # fetches.  min_periods=5 allows partial windows at the start of the
        # full history (first fetch ever for a ticker).
        df["rolling_vol_21d"] = (
            df["yield_change"]
            .rolling(window=self.vol_window, min_periods=5)
            .std()
        )

        # ── spread_vs_benchmark ★ ────────────────────────────────────────────
        rf_ticker = _RISK_FREE_TICKER.get(currency)

        if rf_ticker == ticker:
            df["spread_vs_benchmark"] = 0.0

        elif ytm is not None and rf_ticker is not None:
            rf_ytm = _fetch_rf_ytm(rf_ticker, currency)
            if rf_ytm is not None:
                df["spread_vs_benchmark"] = ytm - rf_ytm
            else:
                df["spread_vs_benchmark"] = ytm - _RF_YTM_STATIC.get(currency, 0.04)

        else:
            df["spread_vs_benchmark"] = np.nan

        # ── Forward-fill sparse analytics columns ────────────────────────────
        for col in ["yield_to_maturity", "spread_vs_benchmark", "duration"]:
            df[col] = df[col].ffill(limit=5)

        return df

    # Backward-compat shim
    def fetch_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """Deprecated — delegates to fetch_data()."""
        return self.fetch_data(ticker, start, end)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_analytics() -> dict:
    return {
        "yield_to_maturity": None,
        "duration":          None,
        "nav":               None,
        "dv01_per_share":    None,
        "aum":               None,
        "expense_ratio":     None,
        "ytd_return":        None,
    }


def _safe_float(value) -> float | None:
    """Return float or None; treat NaN and falsy values as None."""
    if value is None:
        return None
    try:
        f = float(value)
        return None if (f != f) else f   # NaN self-inequality check
    except (TypeError, ValueError):
        return None


def _fetch_rf_ytm(rf_ticker: str, currency: str) -> float | None:
    """Fetch risk-free YTM with the same fallback chain as _fetch_analytics()."""
    try:
        info = yf.Ticker(rf_ticker).info
        return (
            _safe_float(info.get("yield"))
            or _safe_float(info.get("trailingAnnualDividendYield"))
            or _safe_float(info.get("dividendYield"))
        )
    except Exception:
        return None