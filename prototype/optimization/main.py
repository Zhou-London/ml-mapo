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

from _logging import get_logger, run_module

"""--- Import end ---"""

"""--- Config start ---"""
RISK_ADDR     = "tcp://localhost:5556"
FORECAST_ADDR = "tcp://localhost:5557"
TOPIC_COV     = b"COV"
TOPIC_ALPHA   = b"ALPHA"

# ── Risk aversion ────────────────────────────────────────────────────────────
# Utility = alpha'w  -  0.5 * λ * w'Σw
#
# History of this value and why it matters:
#   λ = 50  →  risk term dominates ~50x; optimizer ignores alpha and dumps
#              everything into lowest-variance asset (SGOV).  Produces 2-asset
#              portfolio.
#   λ = 1   →  alpha term dominates; optimizer picks top-alpha assets and
#              caps them all at MAX_WEIGHT.  Produces flat equal-weight result.
#   λ = 5   →  balanced; alpha and risk compete, producing a spread of weights.
#              Tune between 3–10 to taste: higher = more conservative.
RISK_AVERSION = 5.0

# ── Per-asset weight cap ─────────────────────────────────────────────────────
# Without this, unconstrained long-only MVO routinely produces degenerate
# 1–2 asset portfolios.  10% cap forces at least 10 names in the portfolio.
MAX_WEIGHT = 0.10   # 10% max per single asset

# ── Asset-class allocation bands ─────────────────────────────────────────────
# Prevents the optimizer hiding entirely in bonds or FX on any given day.
EQUITY_MIN = 0.30   # at least 30% in equities
BOND_MAX   = 0.40   # at most  40% in bonds
FX_MAX     = 0.10   # at most  10% in FX

# Bond ticker prefixes — must match the market:ticker format used in the snapshot
BOND_PREFIXES = (
    "US:SGOV", "US:BIL", "US:SHY", "US:VGSH", "US:IEF",
    "US:VGIT", "US:TLH", "US:TLT", "US:VGLT", "US:GOVT",
    "UK:IGLT", "UK:VGOV", "UK:GILS", "UK:GLTY", "UK:IGLS",
    "UK:GLTA", "UK:INXG", "EU:IEGA", "EU:VETY", "EU:EXX6",
    "EU:IS04", "EU:EXVM", "EU:IBCI", "EU:IBGL",
)
FX_PREFIX = "FX:"

LONG_ONLY = True
"""--- Config end ---"""

log = get_logger("opt")


# ---------------------------------------------------------------------------
# Asset classification helpers
# ---------------------------------------------------------------------------


def _classify(tickers: list[str]) -> tuple[list[int], list[int], list[int]]:
    """Return (equity_idx, bond_idx, fx_idx) index lists for the ticker list."""
    equity, bond, fx = [], [], []
    for i, t in enumerate(tickers):
        if any(t.startswith(p) for p in BOND_PREFIXES):
            bond.append(i)
        elif t.startswith(FX_PREFIX):
            fx.append(i)
        else:
            equity.append(i)
    return equity, bond, fx


def _asset_label(ticker: str) -> str:
    """Return a short asset-class label for display in the weights table."""
    if any(ticker.startswith(p) for p in BOND_PREFIXES):
        return "bond"
    if ticker.startswith(FX_PREFIX):
        return "fx"
    return "equity"


# ---------------------------------------------------------------------------
# Optimizer
# ---------------------------------------------------------------------------


def mean_variance_optimize(
    alpha: pd.Series,
    cov: pd.DataFrame,
    risk_aversion: float = RISK_AVERSION,
    long_only: bool = LONG_ONLY,
) -> pd.Series:
    """Run mean-variance optimization and return portfolio weights.

    Objective (maximise):
        alpha' w  -  0.5 * risk_aversion * w' Σ w

    Constraints:
        sum(w) = 1
        w[equity] >= EQUITY_MIN
        w[bond]   <= BOND_MAX
        w[fx]     <= FX_MAX
        0 <= w_i  <= MAX_WEIGHT  (long-only)
    """
    tickers = [t for t in alpha.index if t in cov.index]
    if not tickers:
        raise ValueError("no overlap between alpha and covariance tickers")

    mu    = alpha.loc[tickers].to_numpy(dtype=float)
    # .copy() is required — the deserialized DataFrame buffer is read-only,
    # and the regularisation line below does an in-place += that crashes
    # without it.
    sigma = cov.loc[tickers, tickers].to_numpy(dtype=float).copy()
    n     = len(tickers)

    # Regularise: add a tiny diagonal term to ensure positive-definiteness
    # and prevent numerical issues with near-singular covariance matrices
    # (common when bond yield series are highly correlated).
    sigma += np.eye(n) * 1e-6

    def neg_utility(w: np.ndarray) -> float:
        return float(-(mu @ w) + 0.5 * risk_aversion * w @ sigma @ w)

    def neg_utility_jac(w: np.ndarray) -> np.ndarray:
        return -mu + risk_aversion * (sigma @ w)

    equity_idx, bond_idx, fx_idx = _classify(tickers)

    # ── Bounds ───────────────────────────────────────────────────────────────
    if long_only:
        bounds = [(0.0, MAX_WEIGHT)] * n
    else:
        bounds = [(-MAX_WEIGHT, MAX_WEIGHT)] * n

    # ── Constraints ──────────────────────────────────────────────────────────
    constraints: list[dict] = [
        # Weights must sum to 1
        {
            "type": "eq",
            "fun": lambda w: float(w.sum() - 1.0),
            "jac": lambda w: np.ones_like(w),
        },
    ]

    if equity_idx:
        constraints.append({
            "type": "ineq",
            "fun": lambda w, idx=equity_idx: float(w[idx].sum() - EQUITY_MIN),
            "jac": lambda w, idx=equity_idx: np.array(
                [1.0 if i in idx else 0.0 for i in range(n)]
            ),
        })

    if bond_idx:
        constraints.append({
            "type": "ineq",
            "fun": lambda w, idx=bond_idx: float(BOND_MAX - w[idx].sum()),
            "jac": lambda w, idx=bond_idx: np.array(
                [-1.0 if i in idx else 0.0 for i in range(n)]
            ),
        })

    if fx_idx:
        constraints.append({
            "type": "ineq",
            "fun": lambda w, idx=fx_idx: float(FX_MAX - w[idx].sum()),
            "jac": lambda w, idx=fx_idx: np.array(
                [-1.0 if i in idx else 0.0 for i in range(n)]
            ),
        })

    # ── Warm start ───────────────────────────────────────────────────────────
    # Start feasible: respect floor/ceiling from the beginning so SLSQP
    # converges faster and is less likely to land on a local minimum.
    w0 = np.zeros(n)
    if equity_idx:
        w0[equity_idx] = EQUITY_MIN / len(equity_idx)
    remaining = 1.0 - EQUITY_MIN
    if bond_idx:
        bond_alloc = min(remaining * 0.5, BOND_MAX)
        w0[bond_idx] = bond_alloc / len(bond_idx)
        remaining -= bond_alloc
    if fx_idx:
        fx_alloc = min(remaining * 0.5, FX_MAX)
        w0[fx_idx] = fx_alloc / len(fx_idx)
        remaining -= fx_alloc
    # Distribute any leftover back to equities
    if equity_idx and remaining > 1e-9:
        w0[equity_idx] += remaining / len(equity_idx)

    result = minimize(
        neg_utility,
        w0,
        jac=neg_utility_jac,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"ftol": 1e-10, "maxiter": 500},
    )
    if not result.success:
        log.warn(
            "optimizer did not converge",
            message=str(result.message),
            iterations=getattr(result, "nit", None),
        )
    return pd.Series(result.x, index=tickers)


# ---------------------------------------------------------------------------
# ZeroMQ plumbing
# ---------------------------------------------------------------------------


def make_sockets() -> tuple[zmq.Context, zmq.Socket, zmq.Socket, zmq.Poller]:
    """Build SUB sockets for risk and forecast inputs, plus a poller on both."""
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


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def log_weights(weights: pd.Series, *, top: int = 10) -> None:
    """Emit a portfolio summary with ticker, weight, and asset type columns."""
    sorted_w = weights.sort_values(ascending=False)
    nonzero  = int((weights.abs() > 1e-6).sum())
    rows = [
        (str(t), f"{float(w):+.4f}", _asset_label(str(t)))
        for t, w in sorted_w.head(top).items()
        if abs(float(w)) > 1e-6
    ]
    log.table(
        "MVO weights",
        rows,
        headers=("ticker", "weight", "type"),
        assets=len(weights),
        nonzero=nonzero,
        gross=f"{float(weights.abs().sum()):.4f}",
        net=f"{float(weights.sum()):.4f}",
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    """Wait for risk and forecast updates, run MVO, log results."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    with log.pipeline("socket.bind", risk=RISK_ADDR, forecast=FORECAST_ADDR):
        ctx, sub_cov, sub_alpha, poller = make_sockets()

    latest_cov:   dict | None = None
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
                        weights = mean_variance_optimize(
                            latest_alpha["alpha"], latest_cov["covariance"]
                        )
                    except Exception as e:
                        log.exception(
                            "mvo failed",
                            error_type=type(e).__name__,
                            seq=seq,
                        )
                        latest_cov   = None
                        latest_alpha = None
                        continue
                    log_weights(weights)
                latest_cov   = None
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