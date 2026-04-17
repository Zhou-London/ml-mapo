"""--- Import start ---"""

from __future__ import annotations

import pickle
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import zmq
from scipy.optimize import minimize

from _logging import run_module
from snapshots import emit_opt_snapshot, emit_opt_trace, log, log_weights

"""--- Import end ---"""

"""--- Config start ---"""
RISK_ADDR = "tcp://localhost:5556"
FORECAST_ADDR = "tcp://localhost:5557"
TOPIC_COV = b"COV"
TOPIC_ALPHA = b"ALPHA"

RISK_AVERSION = 50.0
LONG_ONLY = True
"""--- Config end ---"""


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
                    emit_opt_snapshot(seq, weights, diagnostics)
                    emit_opt_trace(
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
