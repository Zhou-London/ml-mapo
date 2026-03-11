## Data Ingestion Layer Design

## Stage 1 (Undergoing)
- This is a Data Ingestion layer for multi-asset portfolio.
- Python: pydantic, yfinance, etc.
- Asset/Market: At this stage, only consider US Equity of SP500.
- Analysis: Normalize data with OHLCV.
- Storage: I am thinking in-memory SQLite with ORM.
- Output: IPC with other modules, outputing clean time-series datasets.