"""--- Import start ---"""

from __future__ import annotations

import pickle
import signal
import sys
from abc import ABC, abstractmethod
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import zmq

from _logging import run_module
from config import DATA_ADDR, PUB_ADDR, TOPIC_ALPHA, TOPIC_OHLCV
from snapshots import emit_forecast_snapshot, emit_forecast_trace, log

"""--- Import end ---"""


"""--- Alpha factors start ---

Pluggable strategies consumed by ``AlphaNode``. Subclass ``AlphaFactor`` and
swap the default set in ``main`` to experiment with alternative signals.
"""


class AlphaFactor(ABC):
    """Alpha Factor Interface"""

    name: str = "alpha_factor"

    @abstractmethod
    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series:
        """Given a dict of per-ticker OHLCV DataFrames, return a
        Series of alpha scores indexed by ticker."""


class NaiveMomentumAlpha(AlphaFactor):
    """Example momentum factor: 12-month minus 1-month returns."""

    name = "momentum_12_1"

    def __init__(
        self, lookback: int = 252, skip: int = 21, trading_days: int = 252
    ) -> None:
        self.lookback = lookback
        self.skip = skip
        self.trading_days = trading_days

    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series:
        """12-1 momentum, https://www.gurufocus.com/term/pchange-12-1m"""
        closes = (
            pd.concat({t: df["adj_close"] for t, df in ohlcv.items()}, axis=1)
            .sort_index()
            .dropna(how="all")
        )
        returns = np.log(closes / closes.shift(1)).dropna()
        if self.skip > 0:
            window = returns.iloc[-self.lookback : -self.skip]
        else:
            window = returns.iloc[-self.lookback :]
        return window.mean() * self.trading_days


"""--- Alpha factors end ---"""


"""--- Nodes start ---

Nodes are the stages of the forecast module's transformation pipeline. They
are declared in the order the main loop calls them:

    SubscriberNode → AlphaNode → PublisherNode → ObservabilityNode

Each Node owns its own state and exposes a narrow surface the topology wires
together in ``main``.
"""


class SubscriberNode:
    """Subscribes to the data module's OHLCV PUB and yields decoded snapshots."""

    def __init__(self, addr: str, topic: bytes) -> None:
        self.addr = addr
        self.topic = topic
        self.socket: zmq.Socket | None = None

    def connect(self) -> None:
        self.socket = zmq.Context.instance().socket(zmq.SUB)
        self.socket.connect(self.addr)
        self.socket.setsockopt(zmq.SUBSCRIBE, self.topic)

    def recv(self) -> dict:
        _, payload = self.socket.recv_multipart()
        return pickle.loads(payload)

    def close(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)


class AlphaNode:
    """Scores each AlphaFactor then IR-weighted combines their z-scores into one alpha signal."""

    def __init__(
        self,
        factors: list[AlphaFactor],
        information_ratios: dict[str, float],
    ) -> None:
        self.factors = factors
        self.information_ratios = information_ratios

    def process(
        self, ohlcv: dict[str, pd.DataFrame]
    ) -> tuple[pd.Series, dict[str, pd.Series], dict]:
        """Return ``(combined_alpha, per_factor_scores, combine_trace)``."""
        scores = {f.name: f.score(ohlcv) for f in self.factors}
        alpha, trace = self._ir_weighted_combine(scores)
        return alpha, scores, trace

    def _ir_weighted_combine(
        self, scores: dict[str, pd.Series]
    ) -> tuple[pd.Series, dict]:
        """Combine z-scored factors by IR weights, rescaled to a return magnitude.

        ``trace`` captures every intermediate quantity (cross-section
        means/stds, per-factor z-scores and contributions) needed to
        reconstruct the math for any single asset.
        """
        if not scores:
            raise ValueError("no factor scores to combine")

        tickers: pd.Index | None = None
        for s in scores.values():
            tickers = s.index if tickers is None else tickers.union(s.index)
        assert tickers is not None

        irs = self.information_ratios
        weight_norm = sum(abs(v) for v in irs.values()) or 1.0
        factor_mean = {name: float(s.mean()) for name, s in scores.items()}
        factor_std = {
            name: float(s.std(ddof=0)) if float(s.std(ddof=0)) > 0 else 1.0
            for name, s in scores.items()
        }

        combined = pd.Series(0.0, index=tickers)
        for name, s in scores.items():
            z = (s - factor_mean[name]) / factor_std[name]
            combined = combined.add(
                z * (irs.get(name, 1.0) / weight_norm), fill_value=0.0
            )

        avg_magnitude = float(np.mean([s.abs().mean() for s in scores.values()]))
        trace = {
            "weight_norm": float(weight_norm),
            "avg_magnitude": avg_magnitude,
            "factor_mean": factor_mean,
            "factor_std": factor_std,
            "ir_weights": {
                name: irs.get(name, 1.0) / weight_norm for name in scores
            },
        }
        return combined * avg_magnitude, trace


class PublisherNode:
    """Binds a PUB socket and sends each alpha series as a pickled payload."""

    def __init__(self, addr: str, topic: bytes) -> None:
        self.addr = addr
        self.topic = topic
        self.socket: zmq.Socket | None = None

    def bind(self) -> None:
        self.socket = zmq.Context.instance().socket(zmq.PUB)
        self.socket.bind(self.addr)

    def publish(self, factor_names: list[str], alpha: pd.Series) -> None:
        payload = pickle.dumps(
            {
                "factors": factor_names,
                "tickers": list(alpha.index),
                "alpha": alpha,
            }
        )
        self.socket.send_multipart([self.topic, payload])

    def close(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)


class ObservabilityNode:
    """Folds each cycle's alpha into structured snapshot/trace events."""

    def emit_cycle(
        self,
        seq: int,
        scores: dict[str, pd.Series],
        alpha: pd.Series,
        information_ratios: dict[str, float],
        combine_trace: dict,
    ) -> None:
        emit_forecast_snapshot(seq, scores, alpha, information_ratios)
        emit_forecast_trace(seq, scores, alpha, combine_trace)


"""--- Nodes end ---"""


def main() -> None:
    """Declare the forecast-module pipeline and run the receive → score → publish loop."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    # Topology: SubscriberNode → AlphaNode → PublisherNode → ObservabilityNode
    subscriber = SubscriberNode(DATA_ADDR, TOPIC_OHLCV)
    factors: list[AlphaFactor] = [NaiveMomentumAlpha()]
    # ! IRs come from a backtest; for the demo all factors are equal.
    information_ratios = {f.name: 1.0 for f in factors}
    alpha_node = AlphaNode(factors, information_ratios)
    publisher = PublisherNode(PUB_ADDR, TOPIC_ALPHA)
    observer = ObservabilityNode()

    with log.pipeline("socket.bind", sub=DATA_ADDR, pub=PUB_ADDR):
        subscriber.connect()
        publisher.bind()

    log.info(
        "factors configured",
        factors=[f.name for f in factors],
        information_ratios=information_ratios,
    )

    seq = 0
    try:
        while True:
            data = subscriber.recv()
            seq += 1
            with log.pipeline("alpha.compute", seq=seq):
                try:
                    alpha, scores, combine_trace = alpha_node.process(data["ohlcv"])
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
                observer.emit_cycle(
                    seq, scores, alpha, information_ratios, combine_trace
                )
                publisher.publish(list(scores.keys()), alpha)
                log.info("published", topic=TOPIC_ALPHA.decode(), seq=seq)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", processed=seq)
        subscriber.close()
        publisher.close()
        zmq.Context.instance().term()


if __name__ == "__main__":
    run_module("forecast", main)
