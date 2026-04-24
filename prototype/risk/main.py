"""--- Import start ---"""

from __future__ import annotations

import pickle
import signal
import sys
from abc import ABC, abstractmethod
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import zmq

from _logging import get_logger, run_module

"""--- Import end ---"""

"""--- Config start ---"""
DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5556"
TOPIC_OHLCV = b"OHLCV"
TOPIC_COV = b"COV"

# Asset-class keys — must match data service snapshot keys (lowercase)
AC_EQUITY = "equity"
AC_FX     = "fx"
AC_BOND   = "bond"
"""--- Config end ---"""

# Type alias — mirrors the data service / alpha service definition
AssetSnapshot = dict[str, dict[str, pd.DataFrame]]

log = get_logger("risk")


# ---------------------------------------------------------------------------
# Panel helpers
# ---------------------------------------------------------------------------


def adj_close_panel(equity: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Wide adj-close matrix from the equity sub-snapshot.

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


def yield_change_panel(bonds: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Wide yield_change matrix from the bond sub-snapshot.

    yield_change is the MAPIS daily ΔYTM — the bond equivalent of price
    returns.  Used for bond covariance the same way log-returns are used
    for equities, so the combined covariance matrix is internally consistent.

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


class RiskFactor(ABC):
    """Interface: take {ticker: OHLCV} and return an (n×n) covariance matrix indexed by ticker."""

    name: str = "risk_factor"

    @abstractmethod
    def covariance(self, data: AssetSnapshot) -> pd.DataFrame:
        """Return an annualised covariance matrix indexed and columned by ticker.

        Parameters
        ----------
        data : AssetSnapshot
            Full multi-asset snapshot: { asset_class: { ticker: DataFrame } }

        Returns
        -------
        pd.DataFrame — symmetric (n×n) covariance matrix.
        """



class NaiveRiskFactor(RiskFactor):
    """Sample risk factor"""

    name = "naive_sample_cov"

    def __init__(self, lookback: int = 252, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.trading_days = trading_days

    def covariance(self, data: AssetSnapshot) -> pd.DataFrame:
        """Compute lookback-window sample covariance of equity log-returns, annualised."""
        equity = data.get(AC_EQUITY, {})
        if not equity:
            log.warn("naive_sample_cov: no equity data, returning empty")
            return pd.DataFrame()

        closes  = adj_close_panel(equity)
        returns = np.log(closes / closes.shift(1)).dropna()
        return returns.iloc[-self.lookback :].cov() * self.trading_days


# ---------------------------------------------------------------------------
# Multi-asset covariance  (new)
# ---------------------------------------------------------------------------


class MultiAssetRiskFactor(RiskFactor):
    """Sample covariance across equities AND bonds in a single matrix.

    Equities contribute log-return series.
    Bonds contribute yield_change series (MAPIS ΔYTM).

    Both series are z-scored before computing the joint covariance so that
    the different units (annualised returns vs yield changes) don't distort
    the off-diagonal blocks.  The matrix is then rescaled so the diagonal
    entries are interpretable in their original units.

    This lets the optimiser reason about equity–bond correlations correctly
    — the key benefit of the multi-asset refactor.

    Parameters
    ----------
    lookback : int
        Rolling window in trading days.  Default 252 (1 year).
    trading_days : int
        Annualisation factor.  Default 252.
    """

    name = "multi_asset_sample_cov"

    def __init__(self, lookback: int = 252, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.trading_days = trading_days

    def covariance(self, data: AssetSnapshot) -> pd.DataFrame:
        """Compute joint equity+bond covariance matrix, annualised."""
        series: dict[str, pd.Series] = {}

        # ── Equity log-returns ───────────────────────────────────────────────
        equity = data.get(AC_EQUITY, {})
        if equity:
            closes  = adj_close_panel(equity)
            returns = np.log(closes / closes.shift(1)).dropna()
            window  = returns.iloc[-self.lookback :]
            for ticker in window.columns:
                series[ticker] = window[ticker].dropna()
        else:
            log.warn("multi_asset_sample_cov: no equity data in snapshot")

        # ── Bond yield_change ────────────────────────────────────────────────
        bonds = data.get(AC_BOND, {})
        if bonds:
            yc     = yield_change_panel(bonds)
            window = yc.iloc[-self.lookback :]
            for ticker in window.columns:
                series[ticker] = window[ticker].dropna()
        else:
            log.warn("multi_asset_sample_cov: no bond data in snapshot")

        if not series:
            log.warn("multi_asset_sample_cov: no data for any asset class")
            return pd.DataFrame()

        # ── Align on a common date index ────────────────────────────────────
        # Outer join: NaN where a ticker has no data for a given date
        # (e.g. UK gilt ETFs have no data on US holidays and vice versa)
        combined = pd.DataFrame(series).sort_index()

        # ── Z-score before joining ───────────────────────────────────────────
        # This removes the unit mismatch between equity returns (~2% daily std)
        # and yield changes (~5bps daily std) so the off-diagonal equity–bond
        # block isn't dominated by scale.  We store the per-ticker std so we
        # can rescale back after computing the correlation.
        stds = combined.std(ddof=1).replace(0, np.nan)
        z    = (combined - combined.mean()) / stds

        # ── Correlation matrix (unit-free) ───────────────────────────────────
        corr = z.cov(min_periods=max(20, self.lookback // 10))

        # ── Rescale to covariance in original units ───────────────────────────
        # cov(i, j) = corr(i, j) × std_i × std_j
        std_vec = stds.values
        cov = corr.values * np.outer(std_vec, std_vec)
        cov_df = pd.DataFrame(cov, index=corr.index, columns=corr.columns)

        # ── Annualise ────────────────────────────────────────────────────────
        return cov_df * self.trading_days


# ---------------------------------------------------------------------------
# ZeroMQ plumbing  (unchanged)
# ---------------------------------------------------------------------------


def make_sockets() -> tuple[zmq.Context, zmq.Socket, zmq.Socket]:
    """Build the SUB socket for OHLCV input and the PUB socket for covariance output."""
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(DATA_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_OHLCV)
    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)
    return ctx, sub, pub


def main() -> None:
    """Run the risk loop: receive a snapshot, compute covariance, publish."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    with log.pipeline("socket.bind", sub=DATA_ADDR, pub=PUB_ADDR):
        ctx, sub, pub = make_sockets()

    # Use MultiAssetRiskFactor to get equity+bond covariance.
    # Swap back to NaiveRiskFactor for equity-only if bonds are not yet live.
    factor: RiskFactor = MultiAssetRiskFactor()
    log.info("factor configured", factor=factor.name)

    seq = 0
    try:
        while True:
            _, payload = sub.recv_multipart()
            seq += 1
            with log.pipeline("cov.compute", seq=seq, factor=factor.name):
                try:
                    msg: dict = pickle.loads(payload)

                    # ----------------------------------------------------------
                    # Payload routing — same backward-compat shim as alpha service
                    # New:    { "data": { "equity": {…}, "bond": {…} } }
                    # Legacy: { "ohlcv": { ticker: DataFrame } }
                    # ----------------------------------------------------------
                    if "data" in msg:
                        data: AssetSnapshot = msg["data"]
                    elif "ohlcv" in msg:
                        log.warn(
                            "legacy 'ohlcv' payload; upgrade data service",
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

                    cov = factor.covariance(data)

                except Exception as e:
                    log.exception(
                        "cov computation failed",
                        error_type=type(e).__name__,
                        seq=seq,
                    )
                    continue

                if cov.empty:
                    log.warn("empty covariance matrix; skipping publish", seq=seq)
                    continue

                log.info(
                    "cov ready",
                    shape=f"{cov.shape[0]}x{cov.shape[1]}",
                    tickers=len(cov.index),
                    asset_classes=list(data.keys()),
                )
                pub.send_multipart(
                    [
                        TOPIC_COV,
                        pickle.dumps(
                            {
                                "factor": factor.name,
                                "tickers": list(cov.index),
                                "covariance": cov,
                            }
                        ),
                    ]
                )
                log.info("published", topic=TOPIC_COV.decode(), seq=seq)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", processed=seq)
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    run_module("risk", main)
