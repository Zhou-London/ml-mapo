"""Structured log emitters for the forecast module.

These helpers compose ``log.snapshot`` payloads that dashboards and the CLI
consume; they share the module-level ``log`` instance so stage context set
by ``log.pipeline`` in :mod:`main` is preserved on every emitted event.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from _logging import get_logger

log = get_logger("forecast")


def _series_stats(s: pd.Series, k: int = 5) -> dict:
    """Distributional summary of an alpha/factor series for the dashboard."""
    s_clean = s.dropna()
    if len(s_clean) == 0:
        return {
            "mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0,
            "n": 0, "n_nan": int(len(s) - len(s_clean)),
            "top": [], "bottom": [],
        }
    sorted_s = s_clean.sort_values(ascending=False)
    return {
        "mean": round(float(s_clean.mean()), 6),
        "std": round(float(s_clean.std(ddof=0)), 6),
        "min": round(float(s_clean.min()), 6),
        "max": round(float(s_clean.max()), 6),
        "n": int(len(s_clean)),
        "n_nan": int(len(s) - len(s_clean)),
        "top": [
            {"ticker": str(t), "score": round(float(v), 6)}
            for t, v in sorted_s.head(k).items()
        ],
        "bottom": [
            {"ticker": str(t), "score": round(float(v), 6)}
            for t, v in sorted_s.tail(k).items()
        ],
    }


def emit_forecast_trace(
    seq: int,
    scores: dict[str, pd.Series],
    combined: pd.Series,
    combine_trace: dict,
) -> None:
    """Per-asset pipeline trace through the forecast math.

    For each symbol we expose: the raw factor score, the cross-section
    mean/std that normalized it, its z-score, the IR-weighted contribution,
    and the final combined alpha. This is the full audit trail from raw
    score to alpha for a single name.
    """
    ir_weights = combine_trace["ir_weights"]
    factor_mean = combine_trace["factor_mean"]
    factor_std = combine_trace["factor_std"]
    avg_magnitude = combine_trace["avg_magnitude"]

    assets: dict[str, dict] = {}
    for symbol in combined.index:
        factor_rows = []
        combined_z = 0.0
        for name, s in scores.items():
            if symbol not in s.index:
                continue
            raw = float(s.loc[symbol])
            mean = factor_mean[name]
            std = factor_std[name]
            z = (raw - mean) / std if std > 0 else 0.0
            w = ir_weights[name]
            contrib = z * w
            combined_z += contrib
            factor_rows.append(
                {
                    "name": name,
                    "raw_score": round(raw, 6),
                    "factor_mean": round(mean, 6),
                    "factor_std": round(std, 6),
                    "z_score": round(z, 6),
                    "ir_weight": round(w, 4),
                    "contribution": round(contrib, 6),
                }
            )
        assets[str(symbol)] = {
            "factors": factor_rows,
            "combined_z": round(combined_z, 6),
            "avg_magnitude": round(avg_magnitude, 6),
            "alpha": round(float(combined.loc[symbol]), 6),
        }

    log.snapshot(
        "forecast.trace",
        {
            "seq": seq,
            "weight_norm": combine_trace["weight_norm"],
            "avg_magnitude": avg_magnitude,
            "assets": assets,
        },
    )


def emit_forecast_snapshot(
    seq: int,
    scores: dict[str, pd.Series],
    combined: pd.Series,
    information_ratios: dict[str, float],
) -> None:
    factors = [
        {
            "name": name,
            "information_ratio": float(information_ratios.get(name, 1.0)),
            **_series_stats(s),
        }
        for name, s in scores.items()
    ]
    log.snapshot(
        "forecast.alpha",
        {
            "seq": seq,
            "n_factors": len(factors),
            "factors": factors,
            "combined": _series_stats(combined, k=10),
        },
    )
