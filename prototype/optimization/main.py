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
from snapshots import emit_opt_snapshot, emit_opt_trace, log, log_weights


@register_node("opt/Optimizer")
class OptimizerNode(Node):
    """Solves a mean-variance program with SLSQP + analytic gradient."""

    CATEGORY = "opt"
    INPUTS = {"cov": "covariance", "alpha": "alpha_series"}
    OUTPUTS = {
        "weights": "weights",
        "diagnostics": "dict",
        "cov": "covariance",
        "alpha": "alpha_series",
    }
    PARAMS = {
        "risk_aversion": ("float", RISK_AVERSION),
        "long_only": ("bool", LONG_ONLY),
    }

    def process(self, cov: pd.DataFrame, alpha: pd.Series) -> dict:
        seq = int(self.ctx.get("seq", 0))
        with log.pipeline("mvo.solve", seq=seq):
            weights, diagnostics = self._solve(alpha, cov)
        return {
            "weights": weights,
            "diagnostics": diagnostics,
            "cov": cov,
            "alpha": alpha,
        }

    def _solve(
        self, alpha: pd.Series, cov: pd.DataFrame
    ) -> tuple[pd.Series, dict]:
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
        if not result.success:
            log.warn(
                "optimizer did not converge",
                message=str(result.message),
                iterations=getattr(result, "nit", None),
            )

        weights = result.x
        expected_return = float(mu @ weights)
        portfolio_variance = float(weights @ sigma @ weights)
        portfolio_vol = float(portfolio_variance**0.5)
        utility = expected_return - 0.5 * risk_aversion * portfolio_variance
        diagnostics = {
            "converged": bool(result.success),
            "status": int(getattr(result, "status", -1)),
            "message": str(result.message),
            "iterations": int(getattr(result, "nit", 0)),
            "final_objective": float(result.fun),
            "utility": utility,
            "expected_return": expected_return,
            "portfolio_variance": portfolio_variance,
            "portfolio_vol_annualized": portfolio_vol,
            "sharpe_naive": (
                expected_return / portfolio_vol if portfolio_vol > 0 else 0.0
            ),
            "weight_budget": float(weights.sum()),
            "risk_aversion": risk_aversion,
            "long_only": long_only,
            "n_considered": n,
        }
        return pd.Series(weights, index=tickers), diagnostics


@register_node("opt/Observer")
class ObserverNode(Node):
    """Folds each solve's weights and diagnostics into structured events."""

    CATEGORY = "opt"
    INPUTS = {
        "weights": "weights",
        "alpha": "alpha_series",
        "cov": "covariance",
        "diagnostics": "dict",
    }

    def process(
        self,
        weights: pd.Series,
        alpha: pd.Series,
        cov: pd.DataFrame,
        diagnostics: dict,
    ) -> dict:
        seq = int(self.ctx.get("seq", 0))
        log_weights(weights)
        emit_opt_snapshot(seq, weights, diagnostics)
        emit_opt_trace(seq, weights, alpha, cov, diagnostics)
        return {}
