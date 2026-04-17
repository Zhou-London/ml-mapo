"""Structured log emitters for the optimization module.

These helpers compose ``log.snapshot``/``log.table`` payloads that dashboards
and the CLI consume; they share the module-level ``log`` instance so stage
context set by ``log.pipeline`` in :mod:`main` is preserved on every emitted
event.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from _logging import get_logger

log = get_logger("opt")


def emit_opt_trace(
    seq: int,
    weights: pd.Series,
    alpha: pd.Series,
    cov: pd.DataFrame,
    diagnostics: dict,
) -> None:
    """Per-asset MVO trace.

    For each asset in the solution we record: the alpha that went in, its
    own-variance from the covariance matrix, the final weight, which
    constraint (if any) is binding, and the marginal utility at the
    optimum ( ∂U/∂w_i = μ_i − λ·(Σw)_i ). At an interior optimum that
    gradient is ~0; a large positive value means the long-only floor is
    binding (the asset wants a negative weight it can't take), a large
    negative value means the upper bound is binding.
    """
    tickers = [t for t in alpha.index if t in cov.index]
    if not tickers:
        return
    mu = alpha.loc[tickers].to_numpy(dtype=float)
    sigma = cov.loc[tickers, tickers].to_numpy(dtype=float)
    w = weights.reindex(tickers).fillna(0.0).to_numpy(dtype=float)
    risk_aversion = float(diagnostics["risk_aversion"])
    long_only = bool(diagnostics["long_only"])
    grad = mu - risk_aversion * (sigma @ w)
    sigma_diag = np.diag(sigma)

    lo_bound = 0.0 if long_only else -1.0
    hi_bound = 1.0

    assets: dict[str, dict] = {}
    for i, t in enumerate(tickers):
        wi = float(w[i])
        if abs(wi - lo_bound) < 1e-6 and wi < hi_bound:
            status = "at_lower_bound"
        elif abs(wi - hi_bound) < 1e-6:
            status = "at_upper_bound"
        else:
            status = "interior"
        assets[str(t)] = {
            "alpha_input": round(float(mu[i]), 6),
            "sigma_self": round(float(sigma_diag[i]), 8),
            "vol_annualized_pct": round(float(math.sqrt(max(sigma_diag[i], 0.0)) * 100), 3),
            "final_weight": round(wi, 6),
            "constraint_status": status,
            "marginal_utility": round(float(grad[i]), 6),
            "contribution_to_return": round(float(mu[i] * wi), 6),
            "contribution_to_variance": round(float(wi * (sigma @ w)[i]), 8),
        }

    log.snapshot(
        "opt.trace",
        {
            "seq": seq,
            "risk_aversion": risk_aversion,
            "long_only": long_only,
            "n_assets": len(tickers),
            "assets": assets,
        },
    )


def emit_opt_snapshot(
    seq: int,
    weights: pd.Series,
    diagnostics: dict,
) -> None:
    """Emit the portfolio-solution snapshot for the overview dashboard."""
    nonzero_mask = weights.abs() > 1e-6
    n_assets = int(len(weights))
    n_nonzero = int(nonzero_mask.sum())
    gross = float(weights.abs().sum())
    net = float(weights.sum())
    herfindahl = float((weights ** 2).sum())
    effective_n = float(1.0 / herfindahl) if herfindahl > 0 else 0.0

    sorted_w = weights.sort_values(ascending=False)
    top_holdings = [
        {"ticker": str(t), "weight": round(float(w), 6)}
        for t, w in sorted_w.head(10).items()
    ]
    nonzero_sorted = weights[nonzero_mask].sort_values()
    bottom_holdings = [
        {"ticker": str(t), "weight": round(float(w), 6)}
        for t, w in nonzero_sorted.head(5).items()
    ]

    log.snapshot(
        "opt.solution",
        {
            "seq": seq,
            "convergence": {
                "converged": diagnostics["converged"],
                "status": diagnostics["status"],
                "message": diagnostics["message"],
                "iterations": diagnostics["iterations"],
                "final_objective": round(diagnostics["final_objective"], 8),
            },
            "portfolio": {
                "n_assets": n_assets,
                "n_nonzero": n_nonzero,
                "gross": round(gross, 6),
                "net": round(net, 6),
                "herfindahl": round(herfindahl, 6),
                "effective_n": round(effective_n, 3),
                "max_weight": round(float(weights.max()), 6),
                "min_weight": round(float(weights.min()), 6),
            },
            "metrics": {
                "expected_return": round(diagnostics["expected_return"], 6),
                "portfolio_vol_annualized": round(
                    diagnostics["portfolio_vol_annualized"], 6
                ),
                "utility": round(diagnostics["utility"], 6),
                "sharpe_naive": round(diagnostics["sharpe_naive"], 4),
                "risk_aversion": diagnostics["risk_aversion"],
                "long_only": diagnostics["long_only"],
            },
            "top_holdings": top_holdings,
            "bottom_holdings": bottom_holdings,
            "health": (
                "ok" if diagnostics["converged"] else "not_converged"
            ),
        },
    )


def log_weights(weights: pd.Series, *, top: int = 10) -> None:
    """Emit a portfolio summary plus an aligned top-N weights table."""
    sorted_w = weights.sort_values(ascending=False)
    nonzero = int((weights.abs() > 1e-6).sum())
    rows = [
        (str(t), f"{float(w):+.4f}")
        for t, w in sorted_w.head(top).items()
        if abs(float(w)) > 1e-6
    ]
    log.table(
        "MVO weights",
        rows,
        headers=("ticker", "weight"),
        assets=len(weights),
        nonzero=nonzero,
        gross=f"{float(weights.abs().sum()):.4f}",
        net=f"{float(weights.sum()):.4f}",
    )
