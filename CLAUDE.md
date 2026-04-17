# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

ML-MAPO (Machine Learning Multi-Asset Portfolio Optimizer) is a research prototype by Philip Trealeaven's group at UCL CS. The Python prototype in [prototype/](prototype/) is the primary workload; [web-ui/my-app/](web-ui/my-app/) is a Next.js frontend surface that is still scaffolded.

## Commands

Python toolchain is `uv` (requires Python ≥3.14 per [pyproject.toml](pyproject.toml)). Either activate `.venv` or prefix commands with `uv run`.

- `uv sync` — install/update Python deps
- `uv run python prototype/main.py` — run the full pipeline (supervisor spawns all four modules); Ctrl+C for clean shutdown
- `uv run python prototype/<module>/main.py` — run a single module standalone; downstream modules block on receive until upstream publishes
- `PYTHONUNBUFFERED=1 uv run python -u prototype/main.py 2>&1 | tee run.log` — capture logs when stdout is piped (child output is otherwise block-buffered)
- `cd web-ui/my-app && npm install && npm run dev` — web UI (Next.js 16)
- `cd web-ui/my-app && npm run lint` — ESLint
- No test runner is wired up yet.

### Logging env vars ([prototype/_logging.py](prototype/_logging.py))
- `MAPO_LOG_LEVEL=DEBUG` — raise verbosity (default INFO)
- `MAPO_LOG_FORMAT=json` — newline-delimited JSON instead of the human-readable text format
- `NO_COLOR=1` — disable ANSI colors

### Database
TimescaleDB on `postgresql+psycopg2://postgres:password@localhost:6543/postgres` (hard-coded in [prototype/data/main.py](prototype/data/main.py)). The `OHLCV` table is promoted to a hypertable on `ts`.

## Architecture

See [doc/v0.1_architecture.md](doc/v0.1_architecture.md) for the full spec. Summary:

**Pipeline topology** — four standalone Python programs supervised by [prototype/main.py](prototype/main.py), communicating over ZeroMQ PUB/SUB. There is no shared in-process state.

```
data (5555 OHLCV) → risk (5556 COV) ─┐
                  → forecast (5557 ALPHA) ─┴→ optimization → stdout
```

- **Startup order matters**: subscribers (`opt`, `forecast`, `risk`) are spawned before the publisher (`data`) so SUB sockets are connected before the first publish.
- **Wire format**: ZMQ multipart `[topic_bytes, pickle.dumps(payload)]`. Payloads are dicts carrying pandas DataFrames/Series. All modules must therefore share a Python version.
- **`risk` and `forecast` are parallel** — they never communicate.
- **Lifecycle**: runner spawns children with `start_new_session=True` so terminal Ctrl+C only reaches the runner, which forwards SIGTERM; each child converts SIGTERM→KeyboardInterrupt to close sockets with `linger=0`. Stragglers past `SHUTDOWN_TIMEOUT_S=10s` get SIGKILL.

**Extension points** (abstract bases, pick the default to swap in `<module>/main.py:main`):
- `DataSourceAdaptor` in [prototype/data/adaptors.py](prototype/data/adaptors.py) — implementations: `YfAdaptor`. Contract: inclusive `[start, end]`, return DataFrame with `OHLCV_COLUMNS = ["Open","High","Low","Close","Adj Close","Volume"]` or empty.
- `RiskFactor` in [prototype/risk/main.py](prototype/risk/main.py) — default `NaiveRiskFactor` (annualized sample covariance of daily log returns on `adj_close`, lookback 252).
- `AlphaFactor` in [prototype/forecast/main.py](prototype/forecast/main.py) — default `NaiveMomentumAlpha` (12-1 momentum). Multiple factors combined via `ir_weighted_combine` (z-score → IR-weight → rescale).
- Optimization: `mean_variance_optimize(alpha, cov, λ, long_only)` with SLSQP + analytic gradient; defaults `λ=50`, long-only.

**Data fetching** — `find_missing_ranges` returns at most a leading and trailing gap inside `[start, end]`; interior holes (weekends/holidays) are ignored. Upsert via `session.merge`. The snapshot is re-published every `PUBLISH_INTERVAL_S=5s` so late subscribers still receive data.

**Logging** — all modules use [prototype/_logging.py](prototype/_logging.py). Key primitives:
- `get_logger(module)` and `run_module(module, main_fn)` (the latter wraps every module's `__main__` and handles the crash/interrupt reporting)
- `log.pipeline("stage", **fields)` context manager — stacks stage names, emits start/done/failed with duration
- `log.snapshot(name, data)` — structured state sample (rendered as a dashboard panel in JSON mode; compact summary in text mode)
- `log.table(msg, rows, headers=...)` — indented aligned block
- Each module publishes its own snapshot schema in `<module>/snapshots.py`.

## Web UI caveat

[web-ui/my-app/AGENTS.md](web-ui/my-app/AGENTS.md) (referenced by its CLAUDE.md): this is Next.js 16 with breaking changes from training-data-era Next.js. Read the relevant guide in `node_modules/next/dist/docs/` before writing frontend code.
