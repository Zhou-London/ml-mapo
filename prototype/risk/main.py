"""Risk module.

Subscribes to OHLCV snapshots from the data module, computes a covariance
matrix via a pluggable RiskFactor, and publishes the result so the
optimization module can consume it.
"""

from __future__ import annotations

import pickle
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
import zmq

DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5556"
TOPIC_OHLCV = b"OHLCV"
TOPIC_COV = b"COV"


class RiskFactor(ABC):
    """Interface for a risk factor model.

    Implementations consume a ``{ticker: OHLCV DataFrame}`` mapping and
    return an (n x n) covariance matrix whose rows and columns are tickers.
    Callers are expected to have already filtered the input to the set of
    assets they care about.
    """

    name: str = "risk_factor"

    @abstractmethod
    def covariance(self, ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame: ...


class NaiveRiskFactor(RiskFactor):
    """Annualized sample covariance of daily log returns on adjusted closes.

    This is the textbook baseline — no shrinkage, no factor decomposition —
    and is meant as the default the user can compare custom models against.
    """

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
        window = returns.iloc[-self.lookback :]
        return window.cov() * self.trading_days


def main() -> None:
    ctx = zmq.Context.instance()

    sub = ctx.socket(zmq.SUB)
    sub.connect(DATA_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_OHLCV)

    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)

    print(f"[risk] subscribed to {DATA_ADDR}; publishing on {PUB_ADDR}")

    factor: RiskFactor = NaiveRiskFactor()

    try:
        while True:
            _, payload = sub.recv_multipart()
            data = pickle.loads(payload)
            cov = factor.covariance(data["ohlcv"])
            print(
                f"[risk] {factor.name}: computed {cov.shape[0]}x{cov.shape[1]} "
                f"covariance matrix"
            )
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
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
