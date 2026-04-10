"""--- Import start ---"""

from __future__ import annotations

import pickle
import signal
from abc import ABC, abstractmethod
from collections.abc import Iterable

import numpy as np
import pandas as pd
import zmq

"""--- Import end ---"""

"""--- Config start ---"""

DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5557"
TOPIC_OHLCV = b"OHLCV"
TOPIC_ALPHA = b"ALPHA"

"""--- Config end ---"""


def adj_close_panel(ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Pivots a dict of per-ticker OHLCV DataFrames into a single date
    & ticker adjusted-close panel."""
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
) -> pd.Series:
    """Combine z-scored factors by IR weights, then rescale back to a return magnitude."""
    if not scores:
        raise ValueError("no factor scores to combine")

    tickers: pd.Index | None = None
    for s in scores.values():
        tickers = s.index if tickers is None else tickers.union(s.index)
    assert tickers is not None

    weight_norm = sum(abs(v) for v in information_ratios.values()) or 1.0
    combined = pd.Series(0.0, index=tickers)
    for name, s in scores.items():
        std = s.std(ddof=0)
        z = (s - s.mean()) / (std if std > 0 else 1.0)
        combined = combined.add(
            z * (information_ratios.get(name, 1.0) / weight_norm), fill_value=0.0
        )

    avg_magnitude = float(np.mean([s.abs().mean() for s in scores.values()]))
    return combined * avg_magnitude


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

    ctx, sub, pub = make_sockets()
    print(f"[forecast] subscribed to {DATA_ADDR}; publishing on {PUB_ADDR}")

    factors: Iterable[AlphaFactor] = [NaiveMomentumAlpha()]
    information_ratios = {
        f.name: 1.0 for f in factors
    }  # IRs come from a backtest; for the demo all factors are equal.

    try:
        while True:
            _, payload = sub.recv_multipart()
            data = pickle.loads(payload)

            scores = {f.name: f.score(data["ohlcv"]) for f in factors}
            alpha = ir_weighted_combine(scores, information_ratios)

            print(f"[forecast] computed alpha for {len(alpha)} tickers")
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
    except KeyboardInterrupt:
        pass
    finally:
        print("[forecast] closing sockets")
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
