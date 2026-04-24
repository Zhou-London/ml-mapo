"""--- Import start ---"""

from __future__ import annotations

import pickle
import signal
import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import zmq

from _logging import get_logger, run_module

"""--- Import end ---"""

"""--- Config start ---"""

DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5557"
TOPIC_OHLCV = b"OHLCV"
TOPIC_ALPHA = b"ALPHA"

# Snapshot asset-class keys (lowercase, matching data service output)
AC_EQUITY = "equity"
AC_FX     = "fx"
AC_BOND   = "bond"

"""--- Config end ---"""

# Type alias: multi-asset snapshot from ZeroMQ
# Shape: { "equity": {ticker: DataFrame}, "fx": {…}, "bond": {…} }
AssetSnapshot = dict[str, dict[str, pd.DataFrame]]

log = get_logger("forecast")


# ---------------------------------------------------------------------------
# Panel helpers — one per asset class
# ---------------------------------------------------------------------------


def adj_close_panel(equity: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Wide adj-close price matrix from the equity sub-snapshot.

    Input : data["equity"]  →  { ticker: OHLCV DataFrame }
    Output: DataFrame[date × ticker]
    """
    if not equity:
        return pd.DataFrame()
    return (
        pd.concat({t: df["Adj Close"] for t, df in equity.items()}, axis=1)
        .sort_index()
        .dropna(how="all")
    )


def ytm_panel(bonds: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Wide yield_to_maturity matrix from the bond sub-snapshot.

    Input : data["bond"]  →  { ticker: MAPIS bond DataFrame }
    Output: DataFrame[date × ticker]
    """
    if not bonds:
        return pd.DataFrame()
    return (
        pd.concat({t: df["yield_to_maturity"] for t, df in bonds.items()}, axis=1)
        .sort_index()
        .dropna(how="all")
    )


def yield_change_panel(bonds: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Wide yield_change matrix from the bond sub-snapshot.

    yield_change is the MAPIS daily ΔYTM column — the bond equivalent of
    price returns.  Using the pre-computed column avoids recomputing the
    duration approximation inside the factor.

    Input : data["bond"]  →  { ticker: MAPIS bond DataFrame }
    Output: DataFrame[date × ticker]
    """
    if not bonds:
        return pd.DataFrame()
    return (
        pd.concat({t: df["yield_change"] for t, df in bonds.items()}, axis=1)
        .sort_index()
        .dropna(how="all")
    )


def spread_panel(bonds: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Wide spread_vs_benchmark matrix from the bond sub-snapshot.

    Used by the value / carry factor.

    Input : data["bond"]  →  { ticker: MAPIS bond DataFrame }
    Output: DataFrame[date × ticker]
    """
    if not bonds:
        return pd.DataFrame()
    return (
        pd.concat({t: df["spread_vs_benchmark"] for t, df in bonds.items()}, axis=1)
        .sort_index()
        .dropna(how="all")
    )


class AlphaFactor(ABC):
    """Alpha Factor Interface"""

    name: str = "alpha_factor"

    @abstractmethod
    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series:
        """Given a dict of per-ticker OHLCV DataFrames, return a
        Series of alpha scores indexed by ticker."""


class NaiveMomentumAlpha(AlphaFactor):
    """12-1 price momentum on equities.

    Operates ONLY on data["equity"].
    Reference: https://www.gurufocus.com/term/pchange-12-1m
    """

    name = "momentum_12_1"

    def __init__(
        self, lookback: int = 252, skip: int = 21, trading_days: int = 252
    ) -> None:
        self.lookback = lookback
        self.skip = skip
        self.trading_days = trading_days

    def score(self, data: AssetSnapshot) -> pd.Series:
        equity = data.get(AC_EQUITY, {})
        if not equity:
            log.warn("momentum_12_1: no equity data in snapshot, returning empty")
            return pd.Series(dtype=float)

        closes  = adj_close_panel(equity)
        returns = np.log(closes / closes.shift(1)).dropna()
        if self.skip > 0:
            window = returns.iloc[-self.lookback : -self.skip]
        else:
            window = returns.iloc[-self.lookback :]
        return window.mean() * self.trading_days


# ---------------------------------------------------------------------------
# Bond factors  (MAPIS-aligned — uses pre-computed MAPIS columns directly)
# ---------------------------------------------------------------------------


class YieldMomentumAlpha(AlphaFactor):
    """Yield momentum on bonds — 12-1 analog using MAPIS yield_change column.

    Operates ONLY on data["bond"].

    Uses the pre-computed ``yield_change`` column (daily ΔYTM approximated
    via price return + duration in bond_adaptors.py) rather than recomputing
    it here.  This keeps the factor lean and consistent with the data layer.

    Sign convention (consistent with equity momentum after z-scoring):
        falling yield → negative yield_change → positive score
        rising  yield → positive yield_change → negative score
    The negation makes this a "bond price momentum" signal, not a raw
    yield signal, so ir_weighted_combine can mix it with equity momentum.

    Parameters
    ----------
    lookback : int
        Look-back window in trading days.  Default 63 ≈ 1 quarter.
    skip : int
        Skip the most recent *skip* days (reduces reversal noise).
        Set to 0 to disable.
    """

    name = "yield_momentum"

    def __init__(self, lookback: int = 63, skip: int = 5) -> None:
        self.lookback = lookback
        self.skip     = skip

    def score(self, data: AssetSnapshot) -> pd.Series:
        bonds = data.get(AC_BOND, {})
        if not bonds:
            log.warn("yield_momentum: no bond data in snapshot, returning empty")
            return pd.Series(dtype=float)

        yc = yield_change_panel(bonds)   # daily ΔYTM, pre-computed by adaptor

        if len(yc) < 2:
            log.warn("yield_momentum: insufficient history, returning empty")
            return pd.Series(dtype=float)

        if self.skip > 0:
            window = yc.iloc[-self.lookback : -self.skip]
        else:
            window = yc.iloc[-self.lookback :]

        # Negate: falling yields (negative yield_change) → positive alpha
        return -(window.mean())


class BondValueAlpha(AlphaFactor):
    """Credit spread value factor on bonds — high spread = cheap bond.

    Operates ONLY on data["bond"].  Uses the MAPIS ``spread_vs_benchmark``
    column (fund YTM − regional risk-free YTM, computed in bond_adaptors.py).

    Per the MAPIS README Section 3.5:
        "Value → spread_vs_benchmark (credit spread — high spread = cheap bond)"

    A higher spread signals a relatively cheap bond → positive score.
    The most recent observation is used (the spread is a snapshot broadcast
    across all rows; the last row is the most current).

    Returns pd.Series indexed by ticker.
    """

    name = "bond_value_spread"

    def score(self, data: AssetSnapshot) -> pd.Series:
        bonds = data.get(AC_BOND, {})
        if not bonds:
            log.warn("bond_value_spread: no bond data in snapshot, returning empty")
            return pd.Series(dtype=float)

        sp = spread_panel(bonds)
        if sp.empty:
            return pd.Series(dtype=float)

        # Use the latest available spread per ticker
        return sp.iloc[-1].dropna()


class YieldCurveSteepnessAlpha(AlphaFactor):
    """Curve steepness: spread between a long-end and short-end yield.

    Requires two specific tickers in data["bond"] representing different
    maturities.  The spread is the long-end YTM minus the short-end YTM
    (both from the latest row's yield_to_maturity).

    Returns a two-entry Series:
        long_ticker  →  +spread   (steeper curve = long-end underperforms)
        short_ticker →  −spread

    Enable in main() once you have both tenors in the live bond universe:
        YieldCurveSteepnessAlpha(short_ticker="US:SGOV", long_ticker="US:TLT")
    """

    name = "curve_steepness"

    def __init__(self, short_ticker: str, long_ticker: str) -> None:
        self.short_ticker = short_ticker
        self.long_ticker  = long_ticker

    def score(self, data: AssetSnapshot) -> pd.Series:
        bonds = data.get(AC_BOND, {})
        if self.short_ticker not in bonds or self.long_ticker not in bonds:
            log.warn(
                "curve_steepness: required tickers missing",
                need=[self.short_ticker, self.long_ticker],
                available=list(bonds.keys()),
            )
            return pd.Series(dtype=float)

        short_ytm = bonds[self.short_ticker]["yield_to_maturity"].iloc[-1]
        long_ytm  = bonds[self.long_ticker]["yield_to_maturity"].iloc[-1]
        spread = float(long_ytm - short_ytm)

        return pd.Series(
            {self.short_ticker: -spread, self.long_ticker: spread}
        )


# ---------------------------------------------------------------------------
# Factor combination  (unchanged — union index handles mixed asset classes)
# ---------------------------------------------------------------------------


def ir_weighted_combine(
    scores: dict[str, pd.Series], information_ratios: dict[str, float]
) -> pd.Series:
    """Combine z-scored factors by IR weights, then rescale back to a return magnitude."""
    if not scores:
        raise ValueError("no factor scores to combine")

    tickers: pd.Index | None = None
    for s in scores.values():
        tickers = s.index if tickers is None else tickers.union(s.index)
    assert tickers is not None

    weight_norm = sum(abs(v) for v in information_ratios.values()) or 1.0
    combined = pd.Series(0.0, index=tickers)
    for name, s in scores.items():
        if s.empty:
            continue
        std = s.std(ddof=0)
        z = (s - s.mean()) / (std if std > 0 else 1.0)
        combined = combined.add(
            z * (information_ratios.get(name, 1.0) / weight_norm), fill_value=0.0
        )

    non_empty = [s for s in scores.values() if not s.empty]
    avg_magnitude = float(np.mean([s.abs().mean() for s in non_empty]))
    return combined * avg_magnitude


def make_sockets() -> tuple[zmq.Context, zmq.Socket, zmq.Socket]:
    """Build the SUB socket for OHLCV input and the PUB socket for alpha output."""
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(DATA_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_OHLCV)
    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)
    return ctx, sub, pub


def main() -> None:
    """Receive multi-asset market data, compute alpha, publish results."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    with log.pipeline("socket.bind", sub=DATA_ADDR, pub=PUB_ADDR):
        ctx, sub, pub = make_sockets()

    factors: Iterable[AlphaFactor] = [
        NaiveMomentumAlpha(),       # equity: 12-1 price momentum
        YieldMomentumAlpha(),       # bond:   yield momentum (MAPIS yield_change)
        BondValueAlpha(),           # bond:   spread vs benchmark (MAPIS spread_vs_benchmark)
        # YieldCurveSteepnessAlpha(  # bond:   curve steepness — enable when both tenors live
        #     short_ticker="US:SGOV",
        #     long_ticker="US:TLT",
        # ),
    ]
    information_ratios: dict[str, float] = {
        f.name: 1.0 for f in factors
    }  # ! IRs come from a backtest; for the demo all factors are equal.
    log.info(
        "factors configured",
        factors=[f.name for f in factors],
        information_ratios=information_ratios,
    )

    seq = 0
    try:
        while True:
            _, payload = sub.recv_multipart()
            seq += 1
            with log.pipeline("alpha.compute", seq=seq):
                try:
                    msg: dict = pickle.loads(payload)

                    # ----------------------------------------------------------
                    # Payload routing
                    # New:    { "data": { "equity": {…}, "bond": {…} } }
                    # Legacy: { "ohlcv": { ticker: DataFrame } }
                    #   → wrapped as equity-only so equity factors still work
                    # ----------------------------------------------------------
                    if "data" in msg:
                        data: AssetSnapshot = msg["data"]
                    elif "ohlcv" in msg:
                        log.warn(
                            "legacy 'ohlcv' payload received; "
                            "upgrade data service to emit 'data' key",
                            seq=seq,
                        )
                        data = {AC_EQUITY: msg["ohlcv"]}
                    else:
                        log.error(
                            "unrecognised payload schema",
                            keys=list(msg.keys()),
                            seq=seq,
                        )
                        continue

                    scores = {f.name: f.score(data) for f in factors}
                    # Drop factors that returned no signal this cycle
                    scores = {k: v for k, v in scores.items() if not v.empty}
                    if not scores:
                        log.warn("all factors returned empty scores", seq=seq)
                        continue

                    alpha = ir_weighted_combine(scores, information_ratios)
                except Exception as e:
                    log.exception(
                        "alpha computation failed",
                        error_type=type(e).__name__,
                        seq=seq,
                    )
                    continue
                log.info(
                    "alpha ready",
                    tickers=len(alpha),
                    factors=list(scores.keys()),
                    mean=float(alpha.mean()),
                    std=float(alpha.std(ddof=0)),
                )
                pub.send_multipart(
                    [
                        TOPIC_ALPHA,
                        pickle.dumps(
                            {
                                "factors": list(scores.keys()),
                                "tickers": list(alpha.index),
                                "alpha": alpha,
                            }
                        ),
                    ]
                )
                log.info("published", topic=TOPIC_ALPHA.decode(), seq=seq)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", processed=seq)
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    run_module("forecast", main)
