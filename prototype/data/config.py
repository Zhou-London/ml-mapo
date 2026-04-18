"""Configuration for the data node catalog."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adaptors import DataSourceAdaptor, YfAdaptor


# DB Connection
DB_URL = "postgresql+psycopg2://postgres:password@localhost:6543/postgres"

# Asset class definitions
ASSET_CLASS_EQUITY = "EQUITY"
ASSET_CLASS_FX = "FX"


@dataclass(frozen=True)
class InstrumentUniverse:
    """Defines a set of tradable instruments by asset class, market, and ticker symbol,
    along with the adaptor to fetch their OHLCV data from."""

    asset_class: str
    market: str
    adaptor: DataSourceAdaptor
    tickers: list[str] = field(default_factory=list)


US_EQUITIES = [
    "NVDA",
    "AAPL",
    "MSFT",
    "AMZN",
    "GOOGL",
    "GOOG",
    "AVGO",
    "META",
    "TSLA",
    "WMT",
    "ASML",
    "MU",
    "COST",
    "NFLX",
    "AMD",
    "LRCX",
    "CSCO",
    "AMAT",
    "INTC",
    "PLTR",
    "LIN",
    "KLAC",
    "TMUS",
    "PEP",
    "TXN",
    "AMGN",
    "GILD",
    "ADI",
    "ISRG",
    "ARM",
    "HON",
    "SHOP",
    "PDD",
    "BKNG",
    "QCOM",
    "APP",
    "PANW",
    "WDC",
    "STX",
    "MRVL",
    "VRTX",
    "SBUX",
    "CEG",
    "CMCSA",
    "INTU",
    "CRWD",
    "MAR",
    "ADBE",
    "MELI",
    "REGN",
]
UK_EQUITIES = ["HSBA.L"]
FX_PAIRS = [
    "EURUSD=X",
    "GBPUSD=X",
    "AUDUSD=X",
    "JPYUSD=X",
]

INSTRUMENT_UNIVERSES: list[InstrumentUniverse] = [
    InstrumentUniverse(
        asset_class=ASSET_CLASS_EQUITY,
        market="US",
        adaptor=YfAdaptor(),
        tickers=US_EQUITIES,
    ),
    InstrumentUniverse(
        asset_class=ASSET_CLASS_EQUITY,
        market="UK",
        adaptor=YfAdaptor(),
        tickers=UK_EQUITIES,
    ),
    InstrumentUniverse(
        asset_class=ASSET_CLASS_FX,
        market="FX",
        adaptor=YfAdaptor(),
        tickers=FX_PAIRS,
    ),
]

# Scope definitions
LOOKBACK_DAYS = 365
