"""--- Import start ---"""

from __future__ import annotations

import pickle
import signal
import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import zmq

from _logging import run_module
from snapshots import emit_forecast_snapshot, emit_forecast_trace, log

"""--- Import end ---"""

"""--- Config start ---"""

DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5557"
TOPIC_OHLCV = b"OHLCV"
TOPIC_ALPHA = b"ALPHA"

"""--- Config end ---"""


def adj_close_panel(ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Given a dict of per-ticker OHLCV DataFrames, return a single DataFrame
    of adj_close prices indexed by date and ticker."""
    return (
        pd.concat({t: df["adj_close"] for t, df in ohlcv.items()}, axis=1)
        .sort_index()
        .dropna(how="all")
    )


class AlphaFactor(ABC):
    """Alpha Factor Interface"""

    name: str = "alpha_factor"

    @abstractmethod
    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series:
        """Given a dict of per-ticker OHLCV DataFrames, return a
        Series of alpha scores indexed by ticker."""


class NaiveMomentumAlpha(AlphaFactor):
    """Example momentum factor"""

    name = "momentum_12_1"

    def __init__(
        self, lookback: int = 252, skip: int = 21, trading_days: int = 252
    ) -> None:
        self.lookback = lookback
        self.skip = skip
        self.trading_days = trading_days

    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series:
        """12-1 momentum, https://www.gurufocus.com/term/pchange-12-1m"""
        closes = adj_close_panel(ohlcv)
        returns = np.log(closes / closes.shift(1)).dropna()
        if self.skip > 0:
            window = returns.iloc[-self.lookback : -self.skip]
        else:
            window = returns.iloc[-self.lookback :]
        return window.mean() * self.trading_days


def ir_weighted_combine(
    scores: dict[str, pd.Series], information_ratios: dict[str, float]
) -> tuple[pd.Series, dict]:
    """Combine z-scored factors by IR weights, rescaled to a return magnitude.

    Returns ``(combined_alpha, trace)`` where ``trace`` captures every
    intermediate quantity needed to reconstruct the math for any single
    asset (cross-section means/stds, per-factor z-scores and contributions).
    """
    if not scores:
        raise ValueError("no factor scores to combine")

    tickers: pd.Index | None = None
    for s in scores.values():
        tickers = s.index if tickers is None else tickers.union(s.index)
    assert tickers is not None

    weight_norm = sum(abs(v) for v in information_ratios.values()) or 1.0
    factor_mean = {name: float(s.mean()) for name, s in scores.items()}
    factor_std = {
        name: float(s.std(ddof=0)) if float(s.std(ddof=0)) > 0 else 1.0
        for name, s in scores.items()
    }

    combined = pd.Series(0.0, index=tickers)
    for name, s in scores.items():
        z = (s - factor_mean[name]) / factor_std[name]
        combined = combined.add(
            z * (information_ratios.get(name, 1.0) / weight_norm), fill_value=0.0
        )

    avg_magnitude = float(np.mean([s.abs().mean() for s in scores.values()]))
    trace = {
        "weight_norm": float(weight_norm),
        "avg_magnitude": avg_magnitude,
        "factor_mean": factor_mean,
        "factor_std": factor_std,
        "ir_weights": {
            name: information_ratios.get(name, 1.0) / weight_norm for name in scores
        },
    }
    return combined * avg_magnitude, trace


def make_sockets() -> tuple[zmq.Context, zmq.Socket, zmq.Socket]:
    """Build the SUB socket for OHLCV input and the PUB socket for alpha output."""
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(DATA_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_OHLCV)
    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)
    return ctx, sub, pub


def main() -> None:
    """Main loop: receive OHLCV data, compute alpha, publish results."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    with log.pipeline("socket.bind", sub=DATA_ADDR, pub=PUB_ADDR):
        ctx, sub, pub = make_sockets()

    factors: Iterable[AlphaFactor] = [NaiveMomentumAlpha()]
    information_ratios = {
        f.name: 1.0 for f in factors
    }  # ! IRs come from a backtest; for the demo all factors are equal.
    log.info(
        "factors configured",
        factors=[f.name for f in factors],
        information_ratios=information_ratios,
    )

    seq = 0
    try:
        while True:
            _, payload = sub.recv_multipart()
            seq += 1
            with log.pipeline("alpha.compute", seq=seq):
                try:
                    data = pickle.loads(payload)
                    scores = {f.name: f.score(data["ohlcv"]) for f in factors}
                    alpha, combine_trace = ir_weighted_combine(
                        scores, information_ratios
                    )
                except Exception as e:
                    log.exception(
                        "alpha computation failed",
                        error_type=type(e).__name__,
                        seq=seq,
                    )
                    continue
                log.info(
                    "alpha ready",
                    tickers=len(alpha),
                    factors=list(scores.keys()),
                    mean=float(alpha.mean()),
                    std=float(alpha.std(ddof=0)),
                )
                emit_forecast_snapshot(seq, scores, alpha, information_ratios)
                emit_forecast_trace(seq, scores, alpha, combine_trace)
                pub.send_multipart(
                    [
                        TOPIC_ALPHA,
                        pickle.dumps(
                            {
                                "factors": list(scores.keys()),
                                "tickers": list(alpha.index),
                                "alpha": alpha,
                            }
                        ),
                    ]
                )
                log.info("published", topic=TOPIC_ALPHA.decode(), seq=seq)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", processed=seq)
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    run_module("forecast", main)
