"""--- Import start ---"""

from __future__ import annotations

import pickle
import signal

import numpy as np
import pandas as pd
import zmq
from scipy.optimize import minimize

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
) -> pd.Series:
    """Run mean-variance optimization to compute portfolio weights from alpha and covariance."""
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
        print(f"[opt] WARNING: optimizer did not converge: {result.message}")
    return pd.Series(result.x, index=tickers)


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


def print_weights(weights: pd.Series) -> None:
    """Print the optimized weights in a nice format."""
    print("[opt] MVO weights:")
    width = max((len(str(t)) for t in weights.index), default=6)
    for ticker, w in weights.sort_values(ascending=False).items():
        print(f"  {ticker:>{width}}  {w:+.4f}")


def main() -> None:
    """Main loop: wait for risk and forecast updates, then run MVO and print results."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    ctx, sub_cov, sub_alpha, poller = make_sockets()
    print(f"[opt] listening — risk: {RISK_ADDR}  forecast: {FORECAST_ADDR}")

    latest_cov: dict | None = None
    latest_alpha: dict | None = None

    try:
        while True:
            events = dict(poller.poll(timeout=1000))
            if sub_cov in events:
                _, payload = sub_cov.recv_multipart()
                latest_cov = pickle.loads(payload)
                print(
                    f"[opt] received covariance ({len(latest_cov['tickers'])} tickers)"
                )
            if sub_alpha in events:
                _, payload = sub_alpha.recv_multipart()
                latest_alpha = pickle.loads(payload)
                print(f"[opt] received alpha ({len(latest_alpha['tickers'])} tickers)")

            if latest_cov is not None and latest_alpha is not None:
                weights = mean_variance_optimize(
                    latest_alpha["alpha"], latest_cov["covariance"]
                )
                print_weights(weights)
                latest_cov = None
                latest_alpha = None
    except KeyboardInterrupt:
        pass
    finally:
        print("[opt] closing sockets")
        sub_cov.close(linger=0)
        sub_alpha.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
