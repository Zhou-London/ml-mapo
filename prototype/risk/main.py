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
from config import DATA_ADDR, PUB_ADDR, TOPIC_COV, TOPIC_OHLCV
from snapshots import emit_risk_snapshot, emit_risk_trace, log

"""--- Import end ---"""


"""--- Risk factors start ---

Pluggable strategies consumed by ``CovarianceNode``. Subclass ``RiskFactor``
and swap the default in ``main`` to experiment with alternative covariance
estimators.
"""


class RiskFactor(ABC):
    """Interface: take {ticker: OHLCV} and return an (n×n) covariance matrix indexed by ticker."""

    name: str = "risk_factor"

    @abstractmethod
    def covariance(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame: ...


class NaiveRiskFactor(RiskFactor):
    """Sample risk factor: annualized sample covariance of daily log returns."""

    name = "naive_sample_cov"

    def __init__(self, lookback: int = 252, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.trading_days = trading_days

    def covariance(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Compute the lookback-window sample covariance and annualize it."""
        closes = (
            pd.concat({t: df["adj_close"] for t, df in ohlcv.items()}, axis=1)
            .sort_index()
            .dropna(how="all")
        )
        returns = np.log(closes / closes.shift(1)).dropna()
        return returns.iloc[-self.lookback :].cov() * self.trading_days


"""--- Risk factors end ---"""


"""--- Nodes start ---

Nodes are the stages of the risk module's transformation pipeline. They are
declared in the order the main loop calls them:

    SubscriberNode → CovarianceNode → PublisherNode → ObservabilityNode

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
        """Open a SUB socket on ``self.addr`` subscribed to ``self.topic``."""
        self.socket = zmq.Context.instance().socket(zmq.SUB)
        self.socket.connect(self.addr)
        self.socket.setsockopt(zmq.SUBSCRIBE, self.topic)

    def recv(self) -> dict:
        """Block for one multipart message and return the unpickled payload."""
        _, payload = self.socket.recv_multipart()
        return pickle.loads(payload)

    def close(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)


class CovarianceNode:
    """Applies a ``RiskFactor`` to an OHLCV snapshot to produce a covariance matrix."""

    def __init__(self, factor: RiskFactor) -> None:
        self.factor = factor

    def process(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
        return self.factor.covariance(ohlcv)


class PublisherNode:
    """Binds a PUB socket and sends each covariance snapshot as a pickled payload."""

    def __init__(self, addr: str, topic: bytes) -> None:
        self.addr = addr
        self.topic = topic
        self.socket: zmq.Socket | None = None

    def bind(self) -> None:
        self.socket = zmq.Context.instance().socket(zmq.PUB)
        self.socket.bind(self.addr)

    def publish(self, factor_name: str, cov: pd.DataFrame) -> None:
        payload = pickle.dumps(
            {
                "factor": factor_name,
                "tickers": list(cov.index),
                "covariance": cov,
            }
        )
        self.socket.send_multipart([self.topic, payload])

    def close(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)


class ObservabilityNode:
    """Folds each cycle's covariance into structured snapshot/trace events."""

    def emit_cycle(self, seq: int, factor_name: str, cov: pd.DataFrame) -> None:
        emit_risk_snapshot(seq, factor_name, cov)
        emit_risk_trace(seq, factor_name, cov)


"""--- Nodes end ---"""


def main() -> None:
    """Declare the risk-module pipeline and run the receive → compute → publish loop."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    # Topology: SubscriberNode → CovarianceNode → PublisherNode → ObservabilityNode
    subscriber = SubscriberNode(DATA_ADDR, TOPIC_OHLCV)
    factor: RiskFactor = NaiveRiskFactor()
    cov_node = CovarianceNode(factor)
    publisher = PublisherNode(PUB_ADDR, TOPIC_COV)
    observer = ObservabilityNode()

    with log.pipeline("socket.bind", sub=DATA_ADDR, pub=PUB_ADDR):
        subscriber.connect()
        publisher.bind()

    log.info("factor configured", factor=factor.name)

    seq = 0
    try:
        while True:
            data = subscriber.recv()
            seq += 1
            with log.pipeline("cov.compute", seq=seq, factor=factor.name):
                try:
                    cov = cov_node.process(data["ohlcv"])
                except Exception as e:
                    log.exception(
                        "cov computation failed",
                        error_type=type(e).__name__,
                        seq=seq,
                    )
                    continue
                log.info(
                    "cov ready",
                    shape=f"{cov.shape[0]}x{cov.shape[1]}",
                    tickers=len(cov.index),
                )
                observer.emit_cycle(seq, factor.name, cov)
                publisher.publish(factor.name, cov)
                log.info("published", topic=TOPIC_COV.decode(), seq=seq)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", processed=seq)
        subscriber.close()
        publisher.close()
        zmq.Context.instance().term()


if __name__ == "__main__":
    run_module("risk", main)
