"""Forecast node catalog for the unified ML-MAPO graph."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from graph import Node, register_node


class AlphaFactor(ABC):
    name: str = "alpha_factor"

    @abstractmethod
    def score(self, frame: pd.DataFrame) -> pd.Series: ...


class NaiveMomentumAlpha(AlphaFactor):
    """12-month minus 1-month mean-return momentum on ``adj_close``."""

    name = "momentum_12_1"

    def __init__(self, lookback: int = 252, skip: int = 21, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.skip = skip
        self.trading_days = trading_days

    def score(self, frame: pd.DataFrame) -> pd.Series:
        prices = frame.sort_index().dropna(axis=1, how="all")
        returns = np.log(prices / prices.shift(1)).dropna(how="all")
        if self.skip > 0:
            window = returns.iloc[-self.lookback : -self.skip]
        else:
            window = returns.iloc[-self.lookback :]
        return window.mean() * self.trading_days


_ALPHA_FACTORS: dict[str, type[AlphaFactor]] = {
    NaiveMomentumAlpha.name: NaiveMomentumAlpha,
}


@register_node("forecast/Alpha")
class AlphaNode(Node):
    """Scores each configured AlphaFactor, then IR-weighted combines their z-scores."""

    CATEGORY = "forecast"
    INPUTS = {"frame": "frame"}
    OUTPUTS = {"alpha": "alpha_series"}
    PARAMS = {
        "factors": ("str", NaiveMomentumAlpha.name),
        "information_ratios": ("str", ""),
    }

    def setup(self) -> None:
        names = [s.strip() for s in str(self.params["factors"]).split(",") if s.strip()]
        self._factors: list[AlphaFactor] = [_ALPHA_FACTORS[n]() for n in names]

        ir_raw = str(self.params["information_ratios"]).strip()
        if ir_raw:
            values = [float(x) for x in ir_raw.split(",")]
            if len(values) != len(names):
                raise ValueError(
                    f"information_ratios has {len(values)} values; expected {len(names)}"
                )
            self._information_ratios = dict(zip(names, values))
        else:
            self._information_ratios = {n: 1.0 for n in names}

    def process(self, frame: pd.DataFrame) -> dict:
        scores = {f.name: f.score(frame) for f in self._factors}
        return {"alpha": self._ir_weighted_combine(scores)}

    def _ir_weighted_combine(self, scores: dict[str, pd.Series]) -> pd.Series:
        if not scores:
            raise ValueError("no factor scores to combine")

        tickers: pd.Index | None = None
        for score in scores.values():
            tickers = score.index if tickers is None else tickers.union(score.index)
        assert tickers is not None

        irs = self._information_ratios
        weight_norm = sum(abs(v) for v in irs.values()) or 1.0

        combined = pd.Series(0.0, index=tickers)
        for name, score in scores.items():
            mean = float(score.mean())
            std = float(score.std(ddof=0)) or 1.0
            z = (score - mean) / std
            combined = combined.add(z * (irs.get(name, 1.0) / weight_norm), fill_value=0.0)

        avg_magnitude = float(np.mean([score.abs().mean() for score in scores.values()]))
        return combined * avg_magnitude
