"""Optimization module — graph-driven mean-variance solver.

The module's internal topology lives in ``optimization/graph.json``; this
file defines the node types and a small ``main()`` that loads the graph and
runs ``tick()`` in a loop.
"""

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
from graph import Executor, Node, load_graph, register_node
from snapshots import emit_opt_snapshot, emit_opt_trace, log, log_weights


"""--- Graph nodes start ---"""


@register_node("opt/Subscriber")
class SubscriberNode(Node):
    """Subscribes to risk and forecast PUBs; each tick returns the latest (cov, alpha) pair.

    Buffers the most recently received covariance and alpha separately; once
    both have arrived at least once, ``process`` returns them and resets.
    """

    CATEGORY = "opt"
    OUTPUTS = {"cov": "covariance", "alpha": "alpha_series"}
    PARAMS = {
        "risk_addr": ("str", RISK_ADDR),
        "forecast_addr": ("str", FORECAST_ADDR),
        "topic_cov": ("str", TOPIC_COV.decode()),
        "topic_alpha": ("str", TOPIC_ALPHA.decode()),
        "poll_timeout_ms": ("int", POLL_TIMEOUT_MS),
    }

    def setup(self) -> None:
        ctx = zmq.Context.instance()

        self._sub_cov = ctx.socket(zmq.SUB)
        self._sub_cov.connect(self.params["risk_addr"])
        self._sub_cov.setsockopt(
            zmq.SUBSCRIBE, self.params["topic_cov"].encode()
        )

        self._sub_alpha = ctx.socket(zmq.SUB)
        self._sub_alpha.connect(self.params["forecast_addr"])
        self._sub_alpha.setsockopt(
            zmq.SUBSCRIBE, self.params["topic_alpha"].encode()
        )

        self._poller = zmq.Poller()
        self._poller.register(self._sub_cov, zmq.POLLIN)
        self._poller.register(self._sub_alpha, zmq.POLLIN)

        self._latest_cov: dict | None = None
        self._latest_alpha: dict | None = None

    def process(self) -> dict:
        timeout = int(self.params["poll_timeout_ms"])
        while True:
            events = dict(self._poller.poll(timeout=timeout))
            if self._sub_cov in events:
                _, payload = self._sub_cov.recv_multipart()
                self._latest_cov = pickle.loads(payload)
                log.info(
                    "received cov",
                    tickers=len(self._latest_cov["tickers"]),
                    factor=self._latest_cov.get("factor"),
                )
            if self._sub_alpha in events:
                _, payload = self._sub_alpha.recv_multipart()
                self._latest_alpha = pickle.loads(payload)
                log.info(
                    "received alpha",
                    tickers=len(self._latest_alpha["tickers"]),
                    factors=self._latest_alpha.get("factors"),
                )
            if self._latest_cov is not None and self._latest_alpha is not None:
                cov = self._latest_cov["covariance"]
                alpha = self._latest_alpha["alpha"]
                self._latest_cov = None
                self._latest_alpha = None
                return {"cov": cov, "alpha": alpha}

    def teardown(self) -> None:
        for sock_attr in ("_sub_cov", "_sub_alpha"):
            s = getattr(self, sock_attr, None)
            if s is not None:
                s.close(linger=0)


@register_node("opt/Optimizer")
class OptimizerNode(Node):
    """Solves a long-only (by default) mean-variance program with SLSQP + analytic gradient."""

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
            "risk_aversion": risk_aversion,
            "long_only": long_only,
            "n_considered": n,
        }
        return pd.Series(w, index=tickers), diagnostics


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


"""--- Graph nodes end ---"""


def main() -> None:
    """Load ``optimization/graph.json`` and run its ``tick`` forever."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    graph_path = Path(__file__).parent / "graph.json"
    graph = load_graph(graph_path)
    executor = Executor(graph)
    log.info("graph loaded", path=str(graph_path), nodes=len(graph.nodes))

    with log.pipeline("socket.bind", risk=RISK_ADDR, forecast=FORECAST_ADDR):
        executor.setup()

    try:
        while True:
            executor.tick()
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", solved=int(executor.ctx.get("seq", 0)))
        executor.teardown()
        zmq.Context.instance().term()


if __name__ == "__main__":
    run_module("opt", main)
