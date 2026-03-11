import io
import time
import logging
from datetime import datetime
from urllib.request import Request, urlopen

import pandas as pd
import yfinance as yf
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HISTORY_PERIOD = "1y"
INTERVAL = "1d"
UPDATE_FREQ_SECONDS = 86_400  # 1 day

class OHLCVRecord(BaseModel):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

def fetch_sp500_symbols() -> list[str]:
    req = Request(SP500_URL, headers={"User-Agent": "ml-mapo/0.1"})
    html = urlopen(req).read().decode()
    table = pd.read_html(io.StringIO(html), header=0)[0]
    symbols = table["Symbol"].str.replace(".", "-", regex=False).tolist()
    log.info("Fetched %d SP500 symbols", len(symbols))
    return symbols

def _df_to_records(df: pd.DataFrame, symbol: str) -> list[OHLCVRecord]:
    records = []
    for ts, row in df.iterrows():
        records.append(OHLCVRecord(
            symbol=symbol,
            timestamp=ts.to_pydatetime(),
            open=round(row["Open"], 4),
            high=round(row["High"], 4),
            low=round(row["Low"], 4),
            close=round(row["Close"], 4),
            volume=int(row["Volume"]),
        ))
    return records

def download_batch(symbols: list[str], period: str) -> list[OHLCVRecord]:
    log.info("Downloading %s of data for %d symbols …", period, len(symbols))
    df = yf.download(symbols, period=period, interval=INTERVAL, group_by="ticker", threads=True)
    if df.empty:
        log.warning("yfinance returned empty DataFrame")
        return []

    records: list[OHLCVRecord] = []
    for sym in symbols:
        try:
            sym_df = df[sym].dropna(subset=["Open"])
            records.extend(_df_to_records(sym_df, sym))
        except (KeyError, TypeError):
            log.warning("No data for %s, skipping", sym)
    log.info("Parsed %d records", len(records))
    return records

class OHLCVStore:

    def __init__(self) -> None:
        self.records: list[OHLCVRecord] = []
        self._seen: set[tuple[str, datetime]] = set()

    def append(self, new: list[OHLCVRecord]) -> int:
        added = 0
        for r in new:
            key = (r.symbol, r.timestamp)
            if key not in self._seen:
                self._seen.add(key)
                self.records.append(r)
                added += 1
        return added

    def latest_timestamp(self) -> datetime | None:
        return max((r.timestamp for r in self.records), default=None)

    def __len__(self) -> int:
        return len(self.records)

def run(update_freq: int = UPDATE_FREQ_SECONDS) -> None:
    symbols = fetch_sp500_symbols()
    store = OHLCVStore()

    # --- backfill ---
    backfill = download_batch(symbols, period=HISTORY_PERIOD)
    added = store.append(backfill)
    log.info("Backfill complete: %d records stored", added)

    # --- polling loop ---
    while True:
        log.info("Sleeping %d s until next update …", update_freq)
        time.sleep(update_freq)

        fresh = download_batch(symbols, period="5d")  # small window to catch new bars
        added = store.append(fresh)
        log.info("Update: +%d new records  (total %d)", added, len(store))


if __name__ == "__main__":
    run()
