"""Risk node catalog for the unified ML-MAPO graph."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from graph import Node, register_node


class RiskFactor(ABC):
    """Interface: take a wide adj_close frame and return an (n×n) covariance matrix."""

    name: str = "risk_factor"

    @abstractmethod
    def covariance(self, frame: pd.DataFrame) -> pd.DataFrame: ...


class NaiveRiskFactor(RiskFactor):
    """Annualized sample covariance of daily log returns on ``adj_close``."""

    name = "naive_sample_cov"

    def __init__(self, lookback: int = 252, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.trading_days = trading_days

    def covariance(self, frame: pd.DataFrame) -> pd.DataFrame:
        prices = frame.sort_index().dropna(axis=1, how="all")
        returns = np.log(prices / prices.shift(1)).dropna(how="all")
        return returns.iloc[-self.lookback :].cov() * self.trading_days


_RISK_FACTORS: dict[str, type[RiskFactor]] = {
    NaiveRiskFactor.name: NaiveRiskFactor,
}


@register_node("risk/Covariance")
class CovarianceNode(Node):
    """Applies a RiskFactor to a wide adj_close frame."""

    CATEGORY = "risk"
    INPUTS = {"frame": "frame"}
    OUTPUTS = {"cov": "covariance"}
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

    def process(self, frame: pd.DataFrame) -> dict:
        return {"cov": self._factor.covariance(frame)}
