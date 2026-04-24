"""
db_inspect.py — Quick DB inspection script
==========================================
Run this from the project root AFTER the data service has run at least once.
It prints row counts, date ranges, and sample rows for every relevant table.

Usage:
    python db_inspect.py
    python db_inspect.py --table bond_bar          # single table
    python db_inspect.py --table ohlcv --rows 10   # more sample rows
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy import create_engine, inspect, text

# ── Match the DB_URL in data_main.py exactly ─────────────────────────────────
DB_URL = "postgresql+psycopg2://postgres:6602@localhost:5434/postgres"


def _engine():
    return create_engine(DB_URL, future=True)


def list_tables(engine) -> list[str]:
    return inspect(engine).get_table_names()


def table_summary(engine, table: str, sample_rows: int = 5) -> None:
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"  TABLE: {table}")
    print(sep)

    with engine.connect() as conn:
        # Row count
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"  Rows : {count:,}")

        if count == 0:
            print("  (empty)")
            return

        # Date range (ts column exists on ohlcv and bond_bar)
        cols = {c["name"] for c in inspect(engine).get_columns(table)}
        if "ts" in cols:
            row = conn.execute(
                text(f"SELECT MIN(ts), MAX(ts) FROM {table}")
            ).one()
            print(f"  Date range : {row[0]}  →  {row[1]}")

        # Asset-class breakdown for instruments table
        if table == "instruments":
            rows = conn.execute(
                text(
                    "SELECT asset_class, market, COUNT(*) AS n "
                    "FROM instruments "
                    "GROUP BY asset_class, market "
                    "ORDER BY asset_class, market"
                )
            ).fetchall()
            print(f"\n  Breakdown by asset_class / market:")
            for r in rows:
                print(f"    {r[0]:<10}  {r[1]:<6}  {r[2]:>5} instruments")

        # Ticker coverage for time-series tables
        if table in ("ohlcv", "bond_bar"):
            rows = conn.execute(
                text(
                    f"SELECT i.asset_class, i.market, COUNT(DISTINCT b.instrument_id) AS tickers, "
                    f"COUNT(*) AS bars "
                    f"FROM {table} b "
                    f"JOIN instruments i USING (instrument_id) "
                    f"GROUP BY i.asset_class, i.market "
                    f"ORDER BY i.asset_class, i.market"
                )
            ).fetchall()
            print(f"\n  Coverage (joined with instruments):")
            for r in rows:
                print(
                    f"    {r[0]:<10}  {r[1]:<6}  "
                    f"{r[2]:>4} tickers  {r[3]:>8,} bars"
                )

        # Bond-specific: check MAPIS critical fields
        if table == "bond_bar":
            nulls = conn.execute(
                text(
                    "SELECT "
                    "  COUNT(*) FILTER (WHERE yield_to_maturity IS NULL) AS null_ytm, "
                    "  COUNT(*) FILTER (WHERE yield_change       IS NULL) AS null_yc, "
                    "  COUNT(*) FILTER (WHERE spread_vs_benchmark IS NULL) AS null_spread "
                    "FROM bond_bar"
                )
            ).one()
            print(f"\n  MAPIS critical field NULLs:")
            print(f"    yield_to_maturity   NULL: {nulls[0]:,}")
            print(f"    yield_change        NULL: {nulls[1]:,}")
            print(f"    spread_vs_benchmark NULL: {nulls[2]:,}")

        # Sample rows
        print(f"\n  Sample ({sample_rows} rows):")
        sample = conn.execute(
            text(f"SELECT * FROM {table} LIMIT {sample_rows}")
        ).fetchall()
        col_names = inspect(engine).get_columns(table)
        headers   = [c["name"] for c in col_names]
        print("  " + "  ".join(f"{h:<22}" for h in headers))
        print("  " + "-" * (24 * len(headers)))
        for row in sample:
            print("  " + "  ".join(f"{str(v):<22}" for v in row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect the pipeline DB")
    parser.add_argument("--table", default=None, help="Single table to inspect")
    parser.add_argument("--rows", type=int, default=5, help="Sample rows to print")
    args = parser.parse_args()

    engine = _engine()
    tables = list_tables(engine)

    target_tables = [
        "instruments",
        "equity_details",
        "fx_details",
        "bond_details",
        "ohlcv",
        "bond_bar",
    ]

    if args.table:
        if args.table not in tables:
            print(f"[ERROR] Table '{args.table}' not found. Existing: {tables}")
            sys.exit(1)
        table_summary(engine, args.table, args.rows)
    else:
        for t in target_tables:
            if t in tables:
                table_summary(engine, t, args.rows)
            else:
                print(f"\n[MISSING] Table '{t}' does not exist yet.")

    print("\nDone.\n")


if __name__ == "__main__":
    main()