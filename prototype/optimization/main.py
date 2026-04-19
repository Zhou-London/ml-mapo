"""Optimization node catalog for the unified ML-MAPO graph."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from config import LONG_ONLY, RISK_AVERSION
from graph import Node, register_node


@register_node("opt/Optimizer")
class OptimizerNode(Node):
    """Solves a mean-variance program with SLSQP + analytic gradient."""

    CATEGORY = "opt"
    INPUTS = {"cov": "covariance", "alpha": "alpha_series"}
    OUTPUTS = {"weights": "weights"}
    PARAMS = {
        "risk_aversion": ("float", RISK_AVERSION),
        "long_only": ("bool", LONG_ONLY),
    }

    def process(self, cov: pd.DataFrame, alpha: pd.Series) -> dict:
        risk_aversion = float(self.params["risk_aversion"])
        long_only = bool(self.params["long_only"])

        tickers = [ticker for ticker in alpha.index if ticker in cov.index]
        if not tickers:
            raise ValueError("no overlap between alpha and covariance tickers")

        mu = alpha.loc[tickers].to_numpy(dtype=float)
        sigma = cov.loc[tickers, tickers].to_numpy(dtype=float)
        n = len(tickers)

        def neg_utility(weights: np.ndarray) -> float:
            return float(
                -(mu @ weights) + 0.5 * risk_aversion * weights @ sigma @ weights
            )

        def neg_utility_jac(weights: np.ndarray) -> np.ndarray:
            return -mu + risk_aversion * (sigma @ weights)

        constraints = [
            {
                "type": "eq",
                "fun": lambda weights: float(weights.sum() - 1.0),
                "jac": lambda weights: np.ones_like(weights),
            }
        ]
        bounds = [(0.0, 1.0)] * n if long_only else [(-1.0, 1.0)] * n
        w0 = np.full(n, 1.0 / n)

        result = minimize(
            neg_utility,
            w0,
            jac=neg_utility_jac,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"ftol": 1e-10, "maxiter": 200},
        )
        return {"weights": pd.Series(result.x, index=tickers)}


@register_node("opt/WeightsDisplay")
class WeightsDisplayNode(Node):
    """Prints the optimizer's weights to stdout (surfaced in the editor console)."""

    CATEGORY = "opt"
    INPUTS = {"weights": "weights"}
    OUTPUTS = {"text": "str"}
    PARAMS = {"top": ("int", 0)}  # 0 = show all

    def process(self, weights: pd.Series) -> dict:
        ordered = weights.sort_values(ascending=False)
        top = int(self.params["top"])
        if top > 0:
            ordered = ordered.head(top)

        lines = ["=== Portfolio Weights ==="]
        lines.extend(f"  {sym:<12s}  {w:>8.4f}" for sym, w in ordered.items())
        lines.append(f"  {'SUM':<12s}  {float(weights.sum()):>8.4f}")
        text = "\n".join(lines)
        print(text)
        return {"text": text}
