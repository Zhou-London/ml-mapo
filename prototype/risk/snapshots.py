"""Structured log emitters for the risk module.

These helpers compose ``log.snapshot`` payloads that dashboards and the CLI
consume; they share the module-level ``log`` instance so stage context set
by ``log.pipeline`` in :mod:`main` is preserved on every emitted event.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from _logging import get_logger

log = get_logger("risk")


def emit_risk_snapshot(seq: int, factor_name: str, cov: pd.DataFrame) -> None:
    """Emit a covariance-health snapshot for the overview dashboard."""
    matrix = cov.to_numpy(dtype=float)
    diag = np.diag(matrix)
    # Eigenvalues of a symmetric cov matrix — clipped at 0 for numerical noise.
    eigs = np.linalg.eigvalsh((matrix + matrix.T) / 2.0)
    eig_min = float(eigs.min())
    eig_max = float(eigs.max())
    eig_pos_min = float(eigs[eigs > 0].min()) if (eigs > 0).any() else 0.0
    cond = float(eig_max / eig_pos_min) if eig_pos_min > 0 else float("inf")

    vol_annual = np.sqrt(np.clip(diag, 0.0, None)) * 100.0
    vol_order = np.argsort(vol_annual)[::-1]
    tickers = list(cov.index)
    top_vol = [
        {"ticker": str(tickers[i]), "vol_pct": round(float(vol_annual[i]), 3)}
        for i in vol_order[:10]
    ]
    low_vol = [
        {"ticker": str(tickers[i]), "vol_pct": round(float(vol_annual[i]), 3)}
        for i in vol_order[-5:][::-1]
    ]

    # Off-diagonal correlation magnitude, excluding the identity.
    vol_stdev = np.sqrt(np.clip(diag, 1e-18, None))
    corr = matrix / np.outer(vol_stdev, vol_stdev)
    n = corr.shape[0]
    if n > 1:
        iu = np.triu_indices(n, k=1)
        off_corr = corr[iu]
        corr_abs_mean = float(np.mean(np.abs(off_corr)))
        corr_max = float(np.max(off_corr))
        corr_min = float(np.min(off_corr))
    else:
        corr_abs_mean = corr_max = corr_min = 0.0

    log.snapshot(
        "risk.cov",
        {
            "seq": seq,
            "factor": factor_name,
            "shape": [int(cov.shape[0]), int(cov.shape[1])],
            "eig_min": round(eig_min, 6),
            "eig_max": round(eig_max, 6),
            "eig_pos_min": round(eig_pos_min, 6),
            "condition_number": (
                round(cond, 2) if math.isfinite(cond) else None
            ),
            "negative_eigs": int((eigs < -1e-10).sum()),
            "diag_min": round(float(diag.min()), 6),
            "diag_max": round(float(diag.max()), 6),
            "diag_mean": round(float(diag.mean()), 6),
            "vol_annualized_pct_min": round(float(vol_annual.min()), 3),
            "vol_annualized_pct_max": round(float(vol_annual.max()), 3),
            "off_diag_corr_abs_mean": round(corr_abs_mean, 4),
            "off_diag_corr_max": round(corr_max, 4),
            "off_diag_corr_min": round(corr_min, 4),
            "top_vol": top_vol,
            "low_vol": low_vol,
            "health": (
                "singular" if eig_pos_min <= 0
                else "ill_conditioned" if cond > 1e6
                else "ok"
            ),
        },
    )


def emit_risk_trace(seq: int, factor_name: str, cov: pd.DataFrame) -> None:
    """Per-asset covariance trace: variance/vol plus top/bottom correlates.

    A researcher debugging a name opens this to see why the risk model
    thinks that asset is risky and which other names are driving its row.
    """
    matrix = cov.to_numpy(dtype=float)
    diag = np.diag(matrix)
    vol_stdev = np.sqrt(np.clip(diag, 1e-18, None))
    corr = matrix / np.outer(vol_stdev, vol_stdev)
    tickers = [str(t) for t in cov.index]
    n = len(tickers)

    assets: dict[str, dict] = {}
    for i, t in enumerate(tickers):
        row_corr = corr[i].copy()
        row_corr[i] = -np.inf  # exclude self from "top"
        order = np.argsort(row_corr)[::-1]
        top_corr = [
            {"symbol": tickers[j], "corr": round(float(corr[i, j]), 4)}
            for j in order[:3]
            if j != i
        ]
        row_corr_bot = corr[i].copy()
        row_corr_bot[i] = np.inf
        order_bot = np.argsort(row_corr_bot)
        bot_corr = [
            {"symbol": tickers[j], "corr": round(float(corr[i, j]), 4)}
            for j in order_bot[:3]
            if j != i
        ]
        others = np.delete(corr[i], i)
        assets[t] = {
            "variance": round(float(diag[i]), 8),
            "vol_annualized_pct": round(float(vol_stdev[i] * 100), 3),
            "mean_corr_others": round(float(others.mean()), 4) if n > 1 else 0.0,
            "max_corr_others": round(float(others.max()), 4) if n > 1 else 0.0,
            "min_corr_others": round(float(others.min()), 4) if n > 1 else 0.0,
            "top_corr": top_corr,
            "bottom_corr": bot_corr,
        }

    log.snapshot(
        "risk.trace",
        {"seq": seq, "factor": factor_name, "n_assets": n, "assets": assets},
    )
