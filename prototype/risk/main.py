"""Risk node catalog for the unified ML-MAPO graph."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from graph import Node, register_node
from snapshots import emit_risk_snapshot, emit_risk_trace, log


class RiskFactor(ABC):
    """Interface: take {ticker: OHLCV} and return an (n×n) covariance matrix."""

    name: str = "risk_factor"

    @abstractmethod
    def covariance(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame: ...


class NaiveRiskFactor(RiskFactor):
    """Sample risk factor: annualized sample covariance of daily log returns."""

    name = "naive_sample_cov"

    def __init__(self, lookback: int = 252, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.trading_days = trading_days

    def covariance(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
        closes = (
            pd.concat({t: df["adj_close"] for t, df in ohlcv.items()}, axis=1)
            .sort_index()
            .dropna(how="all")
        )
        returns = np.log(closes / closes.shift(1)).dropna()
        return returns.iloc[-self.lookback :].cov() * self.trading_days


_RISK_FACTORS: dict[str, type[RiskFactor]] = {
    NaiveRiskFactor.name: NaiveRiskFactor,
}


@register_node("risk/Covariance")
class CovarianceNode(Node):
    """Applies a RiskFactor to an OHLCV snapshot."""

    CATEGORY = "risk"
    INPUTS = {"ohlcv": "ohlcv_snapshot"}
    OUTPUTS = {"cov": "covariance", "factor_name": "str"}
    PARAMS = {
        "factor": ("str", NaiveRiskFactor.name),
        "lookback": ("int", 252),
        "trading_days": ("int", 252),
    }

    def setup(self) -> None:
        cls = _RISK_FACTORS[self.params["factor"]]
        self._factor = cls(
            lookback=int(self.params["lookback"]),
            trading_days=int(self.params["trading_days"]),
        )
        log.info("factor configured", factor=self._factor.name)

    def process(self, ohlcv: dict[str, pd.DataFrame]) -> dict:
        seq = int(self.ctx.get("seq", 0))
        with log.pipeline("cov.compute", seq=seq, factor=self._factor.name):
            cov = self._factor.covariance(ohlcv)
            log.info(
                "cov ready",
                shape=f"{cov.shape[0]}x{cov.shape[1]}",
                tickers=len(cov.index),
            )
        return {"cov": cov, "factor_name": self._factor.name}


@register_node("risk/Observer")
class ObserverNode(Node):
    """Folds each cycle's covariance into snapshot/trace events."""

    CATEGORY = "risk"
    INPUTS = {"cov": "covariance", "factor_name": "str"}

    def process(self, cov: pd.DataFrame, factor_name: str) -> dict:
        seq = int(self.ctx.get("seq", 0))
        emit_risk_snapshot(seq, factor_name, cov)
        emit_risk_trace(seq, factor_name, cov)
        return {}
