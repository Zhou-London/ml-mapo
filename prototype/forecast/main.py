"""Forecast module.

Subscribes to OHLCV snapshots from the data module, evaluates one or more
pluggable AlphaFactors, combines their scores, and publishes a forecasted
return vector for the optimization module to consume.
"""

from __future__ import annotations

import pickle
from abc import ABC, abstractmethod
from collections.abc import Iterable

import numpy as np
import pandas as pd
import zmq

DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5557"
TOPIC_OHLCV = b"OHLCV"
TOPIC_ALPHA = b"ALPHA"


class AlphaFactor(ABC):
    """Interface for an alpha factor.

    Implementations consume a ``{ticker: OHLCV DataFrame}`` mapping and
    return a pandas Series of expected returns (annualized) indexed by
    ticker. The composer below z-scores factor scores before combining so
    factors can return either raw returns or unitless signals.
    """

    name: str = "alpha_factor"

    @abstractmethod
    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series: ...


class NaiveMomentumAlpha(AlphaFactor):
    """12-1 momentum: mean daily log-return over the trailing year,
    excluding the most recent month, annualized. A standard baseline.
    """

    name = "momentum_12_1"

    def __init__(self, lookback: int = 252, skip: int = 21, trading_days: int = 252) -> None:
        self.lookback = lookback
        self.skip = skip
        self.trading_days = trading_days

    def score(self, ohlcv: dict[str, pd.DataFrame]) -> pd.Series:
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


def ir_weighted_combine(
    scores: dict[str, pd.Series], information_ratios: dict[str, float]
) -> pd.Series:
    """Combine per-factor scores by information-ratio weighting.

    Each factor's scores are z-scored cross-sectionally, then combined with
    weights proportional to their IRs. With a single factor this degenerates
    to the factor itself (up to rescaling). The result is rescaled to the
    average per-factor magnitude so it can be interpreted as an expected
    return instead of an abstract z-score.
    """

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
        w = information_ratios.get(name, 1.0) / weight_norm
        combined = combined.add(z * w, fill_value=0.0)

    # Rescale z-score-space combination back to a return magnitude.
    avg_mag = float(np.mean([s.abs().mean() for s in scores.values()]))
    return combined * avg_mag


def main() -> None:
    ctx = zmq.Context.instance()

    sub = ctx.socket(zmq.SUB)
    sub.connect(DATA_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_OHLCV)

    pub = ctx.socket(zmq.PUB)
    pub.bind(PUB_ADDR)

    print(f"[forecast] subscribed to {DATA_ADDR}; publishing on {PUB_ADDR}")

    factors: Iterable[AlphaFactor] = [NaiveMomentumAlpha()]
    # In a real system IRs are estimated from a historical backtest; for the
    # demo we assume each factor is equally informative.
    information_ratios = {f.name: 1.0 for f in factors}

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
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
