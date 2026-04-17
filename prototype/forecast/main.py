"""Forecast node catalog for the unified ML-MAPO graph."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from graph import Node, register_node
from snapshots import emit_forecast_snapshot, emit_forecast_trace, log


class AlphaFactor(ABC):
    name: str = "alpha_factor"

    @abstractmethod
    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series: ...


class NaiveMomentumAlpha(AlphaFactor):
    """Example momentum factor: 12-month minus 1-month returns."""

    name = "momentum_12_1"

    def __init__(
        self, lookback: int = 252, skip: int = 21, trading_days: int = 252
    ) -> None:
        self.lookback = lookback
        self.skip = skip
        self.trading_days = trading_days

    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series:
        closes = (
            pd.concat({t: df["adj_close"] for t, df in ohlcv.items()}, axis=1)
            .sort_index()
            .dropna(how="all")
        )
        returns = np.log(closes / closes.shift(1)).dropna()
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
    INPUTS = {"ohlcv": "ohlcv_snapshot"}
    OUTPUTS = {
        "alpha": "alpha_series",
        "scores": "alpha_scores",
        "combine_trace": "dict",
        "information_ratios": "dict",
    }
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

        log.info(
            "factors configured",
            factors=[f.name for f in self._factors],
            information_ratios=self._information_ratios,
        )

    def process(self, ohlcv: dict[str, pd.DataFrame]) -> dict:
        seq = int(self.ctx.get("seq", 0))
        with log.pipeline("alpha.compute", seq=seq):
            scores = {f.name: f.score(ohlcv) for f in self._factors}
            alpha, trace = self._ir_weighted_combine(scores)
            log.info(
                "alpha ready",
                tickers=len(alpha),
                factors=list(scores.keys()),
                mean=float(alpha.mean()),
                std=float(alpha.std(ddof=0)),
            )
        return {
            "alpha": alpha,
            "scores": scores,
            "combine_trace": trace,
            "information_ratios": self._information_ratios,
        }

    def _ir_weighted_combine(
        self, scores: dict[str, pd.Series]
    ) -> tuple[pd.Series, dict]:
        if not scores:
            raise ValueError("no factor scores to combine")

        tickers: pd.Index | None = None
        for score in scores.values():
            tickers = score.index if tickers is None else tickers.union(score.index)
        assert tickers is not None

        irs = self._information_ratios
        weight_norm = sum(abs(v) for v in irs.values()) or 1.0
        factor_mean = {name: float(score.mean()) for name, score in scores.items()}
        factor_std = {
            name: float(score.std(ddof=0)) if float(score.std(ddof=0)) > 0 else 1.0
            for name, score in scores.items()
        }

        combined = pd.Series(0.0, index=tickers)
        for name, score in scores.items():
            z = (score - factor_mean[name]) / factor_std[name]
            combined = combined.add(
                z * (irs.get(name, 1.0) / weight_norm), fill_value=0.0
            )

        avg_magnitude = float(np.mean([score.abs().mean() for score in scores.values()]))
        trace = {
            "weight_norm": float(weight_norm),
            "avg_magnitude": avg_magnitude,
            "factor_mean": factor_mean,
            "factor_std": factor_std,
            "ir_weights": {
                name: irs.get(name, 1.0) / weight_norm for name in scores
            },
        }
        return combined * avg_magnitude, trace


@register_node("forecast/Observer")
class ObserverNode(Node):
    """Folds each cycle's alpha into snapshot/trace events."""

    CATEGORY = "forecast"
    INPUTS = {
        "scores": "alpha_scores",
        "alpha": "alpha_series",
        "information_ratios": "dict",
        "combine_trace": "dict",
    }

    def process(
        self,
        scores: dict[str, pd.Series],
        alpha: pd.Series,
        information_ratios: dict[str, float],
        combine_trace: dict,
    ) -> dict:
        seq = int(self.ctx.get("seq", 0))
        emit_forecast_snapshot(seq, scores, alpha, information_ratios)
        emit_forecast_trace(seq, scores, alpha, combine_trace)
        return {}
