"""Risk module: subscribe to OHLCV, compute a covariance matrix, publish it over ZMQ."""

from __future__ import annotations

import pickle
import signal
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
import zmq

DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5556"
TOPIC_OHLCV = b"OHLCV"
TOPIC_COV = b"COV"


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
    """Annualized sample covariance of daily log-returns on adjusted closes (no shrinkage)."""

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
    # Make SIGTERM raise KeyboardInterrupt so the finally block below runs
    # and the SUB/PUB sockets are closed cleanly before the process exits.
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    ctx, sub, pub = make_sockets()
    print(f"[risk] subscribed to {DATA_ADDR}; publishing on {PUB_ADDR}")

    factor: RiskFactor = NaiveRiskFactor()

    try:
        while True:
            _, payload = sub.recv_multipart()
            data = pickle.loads(payload)
            cov = factor.covariance(data["ohlcv"])
            print(f"[risk] {factor.name}: computed {cov.shape[0]}x{cov.shape[1]} covariance")
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
    except KeyboardInterrupt:
        pass
    finally:
        print("[risk] closing sockets")
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
