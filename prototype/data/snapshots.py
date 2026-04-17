"""Structured log emitters for the data module.

These helpers compose ``log.snapshot`` payloads that dashboards and the CLI
consume; they share the module-level ``log`` instance so stage context set
by ``log.pipeline`` in :mod:`main` is preserved on every emitted event.
"""

from __future__ import annotations

import math
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from _logging import get_logger

log = get_logger("data")


def _asset_health_row(label: str, df: pd.DataFrame) -> dict:
    """Compute per-asset health metrics for the overview dashboard."""
    closes = df["adj_close"]
    first = float(closes.iloc[0])
    last = float(closes.iloc[-1])
    lo = float(closes.min())
    hi = float(closes.max())
    period_ret = ((last / first) - 1.0) * 100.0 if first else 0.0
    log_rets = np.log(closes / closes.shift(1)).dropna()
    vol_annual = (
        float(log_rets.std(ddof=0) * math.sqrt(252) * 100.0) if len(log_rets) else 0.0
    )
    nans = int(df.isna().sum().sum())
    return {
        "symbol": label,
        "bars": int(len(df)),
        "first_bar": df.index.min().isoformat(),
        "last_bar": df.index.max().isoformat(),
        "first_close": round(first, 4),
        "last_close": round(last, 4),
        "lo_close": round(lo, 4),
        "hi_close": round(hi, 4),
        "period_return_pct": round(period_ret, 3),
        "vol_annualized_pct": round(vol_annual, 3),
        "nans": nans,
    }


def emit_data_trace(
    seq: int,
    start: date,
    end: date,
    snapshot: dict[str, pd.DataFrame],
) -> None:
    """Compose the per-cycle data trace snapshot and emit it."""
    assets: dict[str, dict] = {}
    for label, df in snapshot.items():
        closes = df["adj_close"]
        log_rets = np.log(closes / closes.shift(1)).dropna()
        assets[label] = {
            "bars": int(len(df)),
            "first_bar": df.index.min().isoformat(),
            "last_bar": df.index.max().isoformat(),
            "first_close": round(float(closes.iloc[0]), 4),
            "last_close": round(float(closes.iloc[-1]), 4),
            "n_log_returns": int(len(log_rets)),
            "log_return_last": (
                round(float(log_rets.iloc[-1]), 6) if len(log_rets) else 0.0
            ),
            "log_return_mean_daily": (
                round(float(log_rets.mean()), 6) if len(log_rets) else 0.0
            ),
            "log_return_std_daily": (
                round(float(log_rets.std(ddof=0)), 6) if len(log_rets) else 0.0
            ),
            "return_annualized_pct": (
                round(float(log_rets.mean() * 252 * 100), 3) if len(log_rets) else 0.0
            ),
            "vol_annualized_pct": (
                round(float(log_rets.std(ddof=0) * math.sqrt(252) * 100), 3)
                if len(log_rets)
                else 0.0
            ),
        }
    log.snapshot(
        "data.trace",
        {
            "seq": seq,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "assets": assets,
        },
    )


def emit_data_snapshot(
    seq: int,
    start: date,
    end: date,
    stats: dict[str, int],
    snapshot: dict[str, pd.DataFrame],
    payload_bytes: int,
    duration_ms: float,
) -> None:
    """Compose the per-cycle data overview snapshot and emit it."""
    assets = [_asset_health_row(label, df) for label, df in snapshot.items()]
    assets.sort(key=lambda a: a["symbol"])
    total_bars = sum(a["bars"] for a in assets)

    # Highlight symbols that look unhealthy to the researcher.
    problems = [a for a in assets if a["nans"] > 0 or a["vol_annualized_pct"] == 0.0]

    log.snapshot(
        "data.cycle",
        {
            "seq": seq,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "fetch": stats,
            "duration_ms": round(duration_ms, 1),
            "payload_bytes": payload_bytes,
            "total_assets": len(assets),
            "total_bars": total_bars,
            "problem_count": len(problems),
            "assets": assets,
        },
    )
