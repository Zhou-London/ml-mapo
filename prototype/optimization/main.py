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
from config import (
    FORECAST_ADDR,
    LONG_ONLY,
    POLL_TIMEOUT_MS,
    RISK_ADDR,
    RISK_AVERSION,
    TOPIC_ALPHA,
    TOPIC_COV,
)
from snapshots import emit_opt_snapshot, emit_opt_trace, log, log_weights

"""--- Import end ---"""


"""--- Nodes start ---

Nodes are the stages of the optimization module's transformation pipeline.
They are declared in the order the main loop calls them:

    SubscriberNode → OptimizerNode → ObservabilityNode

The optimization module is a pure sink — it consumes cov + alpha and
produces weights + diagnostics for dashboards; there is no downstream PUB.
"""


class SubscriberNode:
    """Subscribes to the risk and forecast PUBs and hands back the latest (cov, alpha) pair.

    Buffers the most recently received covariance and alpha separately; once
    both have arrived at least once, ``wait_for_pair`` returns them and resets,
    matching the original run-when-both-present behavior.
    """

    def __init__(
        self,
        risk_addr: str,
        forecast_addr: str,
        topic_cov: bytes,
        topic_alpha: bytes,
        poll_timeout_ms: int,
    ) -> None:
        self.risk_addr = risk_addr
        self.forecast_addr = forecast_addr
        self.topic_cov = topic_cov
        self.topic_alpha = topic_alpha
        self.poll_timeout_ms = poll_timeout_ms
        self.sub_cov: zmq.Socket | None = None
        self.sub_alpha: zmq.Socket | None = None
        self.poller: zmq.Poller | None = None
        self._latest_cov: dict | None = None
        self._latest_alpha: dict | None = None

    def connect(self) -> None:
        ctx = zmq.Context.instance()

        self.sub_cov = ctx.socket(zmq.SUB)
        self.sub_cov.connect(self.risk_addr)
        self.sub_cov.setsockopt(zmq.SUBSCRIBE, self.topic_cov)

        self.sub_alpha = ctx.socket(zmq.SUB)
        self.sub_alpha.connect(self.forecast_addr)
        self.sub_alpha.setsockopt(zmq.SUBSCRIBE, self.topic_alpha)

        self.poller = zmq.Poller()
        self.poller.register(self.sub_cov, zmq.POLLIN)
        self.poller.register(self.sub_alpha, zmq.POLLIN)

    def wait_for_pair(self) -> tuple[dict, dict]:
        """Block until both a covariance and an alpha have been seen; return them and reset."""
        while True:
            events = dict(self.poller.poll(timeout=self.poll_timeout_ms))
            if self.sub_cov in events:
                _, payload = self.sub_cov.recv_multipart()
                self._latest_cov = pickle.loads(payload)
                log.info(
                    "received cov",
                    tickers=len(self._latest_cov["tickers"]),
                    factor=self._latest_cov.get("factor"),
                )
            if self.sub_alpha in events:
                _, payload = self.sub_alpha.recv_multipart()
                self._latest_alpha = pickle.loads(payload)
                log.info(
                    "received alpha",
                    tickers=len(self._latest_alpha["tickers"]),
                    factors=self._latest_alpha.get("factors"),
                )
            if self._latest_cov is not None and self._latest_alpha is not None:
                cov, alpha = self._latest_cov, self._latest_alpha
                self._latest_cov = None
                self._latest_alpha = None
                return cov, alpha

    def close(self) -> None:
        if self.sub_cov is not None:
            self.sub_cov.close(linger=0)
        if self.sub_alpha is not None:
            self.sub_alpha.close(linger=0)


class OptimizerNode:
    """Solves a long-only (by default) mean-variance program with SLSQP + analytic gradient."""

    def __init__(self, risk_aversion: float, long_only: bool) -> None:
        self.risk_aversion = risk_aversion
        self.long_only = long_only

    def process(
        self, alpha: pd.Series, cov: pd.DataFrame
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
        risk_aversion = self.risk_aversion

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
        bounds = [(0.0, 1.0)] * n if self.long_only else [(-1.0, 1.0)] * n
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
        portfolio_vol = float(portfolio_variance**0.5)
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
            "long_only": bool(self.long_only),
            "n_considered": n,
        }
        return pd.Series(w, index=tickers), diagnostics


class ObservabilityNode:
    """Folds each solve's weights and diagnostics into structured snapshot/trace events."""

    def emit_cycle(
        self,
        seq: int,
        weights: pd.Series,
        alpha: pd.Series,
        cov: pd.DataFrame,
        diagnostics: dict,
    ) -> None:
        log_weights(weights)
        emit_opt_snapshot(seq, weights, diagnostics)
        emit_opt_trace(seq, weights, alpha, cov, diagnostics)


"""--- Nodes end ---"""


def main() -> None:
    """Declare the optimization-module pipeline and run the receive → solve → log loop."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    # Topology: SubscriberNode → OptimizerNode → ObservabilityNode
    subscriber = SubscriberNode(
        RISK_ADDR, FORECAST_ADDR, TOPIC_COV, TOPIC_ALPHA, POLL_TIMEOUT_MS
    )
    optimizer = OptimizerNode(RISK_AVERSION, LONG_ONLY)
    observer = ObservabilityNode()

    with log.pipeline("socket.bind", risk=RISK_ADDR, forecast=FORECAST_ADDR):
        subscriber.connect()

    seq = 0
    try:
        while True:
            cov_payload, alpha_payload = subscriber.wait_for_pair()
            seq += 1
            with log.pipeline("mvo.solve", seq=seq):
                try:
                    weights, diagnostics = optimizer.process(
                        alpha_payload["alpha"], cov_payload["covariance"]
                    )
                except Exception as e:
                    log.exception(
                        "mvo failed",
                        error_type=type(e).__name__,
                        seq=seq,
                    )
                    continue
                observer.emit_cycle(
                    seq,
                    weights,
                    alpha_payload["alpha"],
                    cov_payload["covariance"],
                    diagnostics,
                )
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", solved=seq)
        subscriber.close()
        zmq.Context.instance().term()


if __name__ == "__main__":
    run_module("opt", main)
