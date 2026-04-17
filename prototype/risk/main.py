"""Risk module — graph-driven receive/compute/publish pipeline.

The module's internal topology lives in ``risk/graph.json``; this file
defines the node types and a small ``main()`` that loads the graph and runs
``tick()`` in a loop.
"""

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
from graph import Executor, Node, load_graph, register_node
from snapshots import emit_risk_snapshot, emit_risk_trace, log


"""--- Risk factors start ---

Pluggable strategies consumed by ``CovarianceNode``. Subclass ``RiskFactor``
and swap the default in ``risk/graph.json`` (via the Covariance node's
``factor`` param) to experiment with alternative covariance estimators.
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
        closes = (
            pd.concat({t: df["adj_close"] for t, df in ohlcv.items()}, axis=1)
            .sort_index()
            .dropna(how="all")
        )
        returns = np.log(closes / closes.shift(1)).dropna()
        return returns.iloc[-self.lookback :].cov() * self.trading_days


_RISK_FACTORS: dict[str, type[RiskFactor]] = {
    NaiveRiskFactor.name: NaiveRiskFactor,
}


"""--- Risk factors end ---"""


"""--- Graph nodes start ---"""


@register_node("risk/Subscriber")
class SubscriberNode(Node):
    """Receives OHLCV snapshots from the data module's PUB and outputs them per tick."""

    CATEGORY = "risk"
    OUTPUTS = {"ohlcv": "ohlcv_snapshot"}
    PARAMS = {
        "addr": ("str", DATA_ADDR),
        "topic": ("str", TOPIC_OHLCV.decode()),
    }

    def setup(self) -> None:
        self._socket = zmq.Context.instance().socket(zmq.SUB)
        self._socket.connect(self.params["addr"])
        self._socket.setsockopt(zmq.SUBSCRIBE, self.params["topic"].encode())

    def process(self) -> dict:
        _, payload = self._socket.recv_multipart()
        data = pickle.loads(payload)
        return {"ohlcv": data["ohlcv"]}

    def teardown(self) -> None:
        if getattr(self, "_socket", None) is not None:
            self._socket.close(linger=0)


@register_node("risk/Covariance")
class CovarianceNode(Node):
    """Applies a ``RiskFactor`` to an OHLCV snapshot to produce a covariance matrix."""

    CATEGORY = "risk"
    INPUTS = {"ohlcv": "ohlcv_snapshot"}
    OUTPUTS = {"cov": "covariance", "factor_name": "str"}
    PARAMS = {
        "factor": ("str", NaiveRiskFactor.name),
        "lookback": ("int", 252),
        "trading_days": ("int", 252),
    }

    def setup(self) -> None:
        cls = _RISK_FACTORS[self.params["factor"]]
        self._factor = cls(
            lookback=int(self.params["lookback"]),
            trading_days=int(self.params["trading_days"]),
        )
        log.info("factor configured", factor=self._factor.name)

    def process(self, ohlcv: dict[str, pd.DataFrame]) -> dict:
        seq = int(self.ctx.get("seq", 0))
        with log.pipeline("cov.compute", seq=seq, factor=self._factor.name):
            cov = self._factor.covariance(ohlcv)
            log.info(
                "cov ready",
                shape=f"{cov.shape[0]}x{cov.shape[1]}",
                tickers=len(cov.index),
            )
        return {"cov": cov, "factor_name": self._factor.name}


@register_node("risk/Publisher")
class PublisherNode(Node):
    """Binds a PUB socket and sends each covariance matrix as a pickled payload."""

    CATEGORY = "risk"
    INPUTS = {"cov": "covariance", "factor_name": "str"}
    PARAMS = {
        "addr": ("str", PUB_ADDR),
        "topic": ("str", TOPIC_COV.decode()),
    }

    def setup(self) -> None:
        self._socket = zmq.Context.instance().socket(zmq.PUB)
        self._socket.bind(self.params["addr"])

    def process(self, cov: pd.DataFrame, factor_name: str) -> dict:
        payload = pickle.dumps(
            {
                "factor": factor_name,
                "tickers": list(cov.index),
                "covariance": cov,
            }
        )
        self._socket.send_multipart([self.params["topic"].encode(), payload])
        log.info("published", topic=self.params["topic"], seq=int(self.ctx.get("seq", 0)))
        return {}

    def teardown(self) -> None:
        if getattr(self, "_socket", None) is not None:
            self._socket.close(linger=0)


@register_node("risk/Observer")
class ObserverNode(Node):
    """Folds each cycle's covariance into snapshot/trace events."""

    CATEGORY = "risk"
    INPUTS = {"cov": "covariance", "factor_name": "str"}

    def process(self, cov: pd.DataFrame, factor_name: str) -> dict:
        seq = int(self.ctx.get("seq", 0))
        emit_risk_snapshot(seq, factor_name, cov)
        emit_risk_trace(seq, factor_name, cov)
        return {}


"""--- Graph nodes end ---"""


def main() -> None:
    """Load ``risk/graph.json`` and run its ``tick`` forever."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    graph_path = Path(__file__).parent / "graph.json"
    graph = load_graph(graph_path)
    executor = Executor(graph)
    log.info("graph loaded", path=str(graph_path), nodes=len(graph.nodes))

    with log.pipeline("socket.bind", sub=DATA_ADDR, pub=PUB_ADDR):
        executor.setup()

    try:
        while True:
            executor.tick()
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", processed=int(executor.ctx.get("seq", 0)))
        executor.teardown()
        zmq.Context.instance().term()


if __name__ == "__main__":
    run_module("risk", main)
