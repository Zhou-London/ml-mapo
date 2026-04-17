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
from snapshots import emit_risk_snapshot, emit_risk_trace, log

"""--- Import end ---"""

"""--- Config start ---"""
DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5556"
TOPIC_OHLCV = b"OHLCV"
TOPIC_COV = b"COV"
"""--- Config end ---"""


def adj_close_panel(ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Stack per-ticker OHLCV frames into a single DataFrame of adjusted closes."""
    return (
        pd.concat({t: df["adj_close"] for t, df in ohlcv.items()}, axis=1)
        .sort_index()
        .dropna(how="all")
    )


class RiskFactor(ABC):
    """Interface: take {ticker: OHLCV} and return an (n×n) covariance matrix indexed by ticker."""

    name: str = "risk_factor"

    @abstractmethod
    def covariance(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame: ...


class NaiveRiskFactor(RiskFactor):
    """Sample risk factor"""

    name = "naive_sample_cov"

    def __init__(self, lookback: int = 252, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.trading_days = trading_days

    def covariance(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Compute the lookback-window sample covariance and annualize it."""
        closes = adj_close_panel(ohlcv)
        returns = np.log(closes / closes.shift(1)).dropna()
        return returns.iloc[-self.lookback :].cov() * self.trading_days


def make_sockets() -> tuple[zmq.Context, zmq.Socket, zmq.Socket]:
    """Build the SUB socket for OHLCV input and the PUB socket for covariance output."""
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(DATA_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_OHLCV)
    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)
    return ctx, sub, pub


def main() -> None:
    """Run the risk loop: receive a snapshot, compute covariance, publish."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    with log.pipeline("socket.bind", sub=DATA_ADDR, pub=PUB_ADDR):
        ctx, sub, pub = make_sockets()

    factor: RiskFactor = NaiveRiskFactor()
    log.info("factor configured", factor=factor.name)

    seq = 0
    try:
        while True:
            _, payload = sub.recv_multipart()
            seq += 1
            with log.pipeline("cov.compute", seq=seq, factor=factor.name):
                try:
                    data = pickle.loads(payload)
                    cov = factor.covariance(data["ohlcv"])
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
                emit_risk_snapshot(seq, factor.name, cov)
                emit_risk_trace(seq, factor.name, cov)
                pub.send_multipart(
                    [
                        TOPIC_COV,
                        pickle.dumps(
                            {
                                "factor": factor.name,
                                "tickers": list(cov.index),
                                "covariance": cov,
                            }
                        ),
                    ]
                )
                log.info("published", topic=TOPIC_COV.decode(), seq=seq)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing sockets", processed=seq)
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    run_module("risk", main)
