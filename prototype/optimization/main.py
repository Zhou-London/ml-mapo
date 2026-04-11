"""--- Import start ---"""

from __future__ import annotations

import math
import pickle
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import zmq
from scipy.optimize import minimize

from _logging import get_logger, run_module

"""--- Import end ---"""

"""--- Config start ---"""
RISK_ADDR = "tcp://localhost:5556"
FORECAST_ADDR = "tcp://localhost:5557"
TOPIC_COV = b"COV"
TOPIC_ALPHA = b"ALPHA"

RISK_AVERSION = 50.0
LONG_ONLY = True
"""--- Config end ---"""

log = get_logger("opt")


def mean_variance_optimize(
    alpha: pd.Series,
    cov: pd.DataFrame,
    risk_aversion: float = RISK_AVERSION,
    long_only: bool = LONG_ONLY,
) -> tuple[pd.Series, dict]:
    """Run mean-variance optimization.

    Returns (weights, diagnostics). Diagnostics exposes scipy's convergence
    state plus portfolio-level metrics so the dashboard can surface them.
    """
    tickers = [t for t in alpha.index if t in cov.index]
    if not tickers:
        raise ValueError("no overlap between alpha and covariance tickers")

    mu = alpha.loc[tickers].to_numpy(dtype=float)
    sigma = cov.loc[tickers, tickers].to_numpy(dtype=float)
    n = len(tickers)

    def neg_utility(w: np.ndarray) -> float:
        return float(-(mu @ w) + 0.5 * risk_aversion * w @ sigma @ w)

    def neg_utility_jac(w: np.ndarray) -> np.ndarray:
        return -mu + risk_aversion * (sigma @ w)

    constraints = [
        {
            "type": "eq",
            "fun": lambda w: float(w.sum() - 1.0),
            "jac": lambda w: np.ones_like(w),
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

    w = result.x
    expected_return = float(mu @ w)
    portfolio_variance = float(w @ sigma @ w)
    portfolio_vol = float(portfolio_variance ** 0.5)
    utility = expected_return - 0.5 * risk_aversion * portfolio_variance
    weight_budget = float(w.sum())
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
        "weight_budget": weight_budget,
        "risk_aversion": float(risk_aversion),
        "long_only": bool(long_only),
        "n_considered": n,
    }
    return pd.Series(w, index=tickers), diagnostics


def make_sockets() -> tuple[zmq.Context, zmq.Socket, zmq.Socket, zmq.Poller]:
    """Build SUB sockets for risk and forecast inputs, plus a poller registered on both."""
    ctx = zmq.Context.instance()

    sub_cov = ctx.socket(zmq.SUB)
    sub_cov.connect(RISK_ADDR)
    sub_cov.setsockopt(zmq.SUBSCRIBE, TOPIC_COV)

    sub_alpha = ctx.socket(zmq.SUB)
    sub_alpha.connect(FORECAST_ADDR)
    sub_alpha.setsockopt(zmq.SUBSCRIBE, TOPIC_ALPHA)

    poller = zmq.Poller()
    poller.register(sub_cov, zmq.POLLIN)
    poller.register(sub_alpha, zmq.POLLIN)

    return ctx, sub_cov, sub_alpha, poller


def _emit_opt_trace(
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


def _emit_opt_snapshot(
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


def main() -> None:
    """Main loop: wait for risk and forecast updates, then run MVO and log results."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    with log.pipeline("socket.bind", risk=RISK_ADDR, forecast=FORECAST_ADDR):
        ctx, sub_cov, sub_alpha, poller = make_sockets()

    latest_cov: dict | None = None
    latest_alpha: dict | None = None
    seq = 0

    try:
        while True:
            events = dict(poller.poll(timeout=1000))
            if sub_cov in events:
                _, payload = sub_cov.recv_multipart()
                latest_cov = pickle.loads(payload)
                log.info(
                    "received cov",
                    tickers=len(latest_cov["tickers"]),
                    factor=latest_cov.get("factor"),
                )
            if sub_alpha in events:
                _, payload = sub_alpha.recv_multipart()
                latest_alpha = pickle.loads(payload)
                log.info(
                    "received alpha",
                    tickers=len(latest_alpha["tickers"]),
                    factors=latest_alpha.get("factors"),
                )

            if latest_cov is not None and latest_alpha is not None:
                seq += 1
                with log.pipeline("mvo.solve", seq=seq):
                    try:
                        weights, diagnostics = mean_variance_optimize(
                            latest_alpha["alpha"], latest_cov["covariance"]
                        )
                    except Exception as e:
                        log.exception(
                            "mvo failed",
                            error_type=type(e).__name__,
                            seq=seq,
                        )
                        latest_cov = None
                        latest_alpha = None
                        continue
                    log_weights(weights)
                    _emit_opt_snapshot(seq, weights, diagnostics)
                    _emit_opt_trace(
                        seq,
                        weights,
                        latest_alpha["alpha"],
                        latest_cov["covariance"],
                        diagnostics,
                    )
                latest_cov = None
                latest_alpha = None
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", solved=seq)
        sub_cov.close(linger=0)
        sub_alpha.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    run_module("opt", main)
