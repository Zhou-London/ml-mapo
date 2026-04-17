# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

ML-MAPO (Machine Learning Multi-Asset Portfolio Optimizer) is a research prototype by Philip Trealeaven's group at UCL CS. Two stacks live side by side:

- [prototype/](prototype/) — the Python pipeline, primary workload.
- [web-ui/my-app/](web-ui/my-app/) — a Next.js 16 + React 19 editor that reads and writes each module's `graph.json`.

## Commands

Python toolchain is `uv` (requires Python ≥3.14 per [pyproject.toml](pyproject.toml)). Either activate `.venv` or prefix commands with `uv run`.

- `uv sync` — install/update Python deps.
- `uv run python prototype/main.py` — run the full pipeline (supervisor spawns all four modules); Ctrl+C for clean shutdown.
- `uv run python prototype/<module>/main.py` — run a single module standalone; downstream modules block on receive until upstream publishes.
- `uv run python prototype/graph_cli.py schemas` — dump every `@register_node`-decorated class as JSON (palette catalog / debugging).
- `PYTHONUNBUFFERED=1 uv run python -u prototype/main.py 2>&1 | tee run.log` — capture logs when stdout is piped (child output is otherwise block-buffered).
- `cd web-ui/my-app && npm install && npm run dev` — Next.js editor (http://localhost:3000/graph).
- `cd web-ui/my-app && npm run build` — production build (also runs `tsc`).
- `cd web-ui/my-app && npm run lint` — ESLint via `next lint`.
- `cd web-ui/my-app && PORT=3000 node tests/smoke.mjs` — headless-Chromium end-to-end test (requires `npx playwright install chromium` once). No Python test suite is wired up yet.

### Logging env vars ([prototype/_logging.py](prototype/_logging.py))
- `MAPO_LOG_LEVEL=DEBUG` — raise verbosity (default INFO).
- `MAPO_LOG_FORMAT=json` — newline-delimited JSON instead of the human-readable text format.
- `NO_COLOR=1` — disable ANSI colors.

### Database
TimescaleDB at `postgresql+psycopg2://postgres:password@localhost:6543/postgres` (hard-coded in [prototype/data/config.py](prototype/data/config.py)). The `ohlcv` table is promoted to a hypertable on `ts`.

## Pipeline architecture

See [doc/v0.1_architecture.md](doc/v0.1_architecture.md) for the full spec.

**Topology** — four standalone Python programs supervised by [prototype/main.py](prototype/main.py), communicating over ZeroMQ PUB/SUB. There is no shared in-process state.

```
data (5555 OHLCV) → risk (5556 COV) ─┐
                  → forecast (5557 ALPHA) ─┴→ optimization → stdout
```

- **Startup order matters**: subscribers (`opt`, `forecast`, `risk`) are spawned before the publisher (`data`) so SUB sockets are connected before the first publish.
- **Wire format**: ZMQ multipart `[topic_bytes, pickle.dumps(payload)]`. Payloads are dicts carrying pandas DataFrames/Series, so every module must share a Python version.
- **`risk` and `forecast` are parallel** — they never communicate.
- **Lifecycle**: the runner spawns children with `start_new_session=True` so terminal Ctrl+C reaches only the runner, which forwards SIGTERM; each child converts SIGTERM→KeyboardInterrupt to close sockets with `linger=0`. Stragglers past `SHUTDOWN_TIMEOUT_S=10s` get SIGKILL.

## Graph system (important)

Each pipeline module is itself a DAG. The runtime lives in [prototype/graph.py](prototype/graph.py); each module's `main.py` defines `Node` subclasses, and its `<module>/graph.json` wires them together.

- **Adding a node type**: subclass `graph.Node`, declare `INPUTS` / `OUTPUTS` / `PARAMS` / `CATEGORY` as class attributes, implement `process(**inputs) -> dict`, and decorate with `@register_node("<module>/TypeName")`. Optional `setup()` / `teardown()` lifecycle hooks. Per-tick metadata (`seq`, `t0`) is passed via `self.ctx`.
- **Changing a node's behavior**: edit its `params` entry in `<module>/graph.json` — or the Node subclass if you need new ports. Do *not* hunt for an imperative `main()` to patch; the module's `main.py` only builds the graph and calls `Executor.tick()` forever.
- **On-disk format**: `{nodes: [{id, type, params, pos}], edges: [{src_node, src_port, dst_node, dst_port}]}`. Ports are referenced by name — the JSON is *independent of the UI's slot ordering*.
- **Default factor plugins**:
  - `RiskFactor` in [prototype/risk/main.py](prototype/risk/main.py) — default `NaiveRiskFactor` (annualized sample covariance of daily log returns on `adj_close`, lookback 252). Select via the `factor` param on `risk/Covariance`.
  - `AlphaFactor` in [prototype/forecast/main.py](prototype/forecast/main.py) — default `NaiveMomentumAlpha` (12-1 momentum). The `factors` / `information_ratios` params on `forecast/Alpha` accept CSVs; multiple factors combine via `_ir_weighted_combine` (z-score → IR-weight → rescale).
  - Optimization: `opt/Optimizer` runs `scipy.optimize.minimize(SLSQP)` with an analytic gradient; defaults `risk_aversion=50`, `long_only=true`.
- **Empty scaffold**: [prototype/backtest/](prototype/backtest/) exists but its `main.py` is empty — nothing to wire yet.

## Logging

Every module goes through [prototype/_logging.py](prototype/_logging.py):

- `get_logger(module)` and `run_module(module, main_fn)` — the latter wraps every `__main__` and handles crash/interrupt reporting.
- `log.pipeline("stage", **fields)` context manager — stacks stage names, emits start/done/failed with duration.
- `log.snapshot(name, data)` — structured state sample (dashboard panel in JSON mode; compact summary in text mode).
- `log.table(msg, rows, headers=...)` — indented aligned block.
- Each module publishes its snapshot schemas in `<module>/snapshots.py`.

## Web UI (Next.js 16 + LiteGraph)

[web-ui/my-app/](web-ui/my-app/) is an app-router Next.js 16 project that edits each module's `graph.json`. ComfyUI's canvas library ([@comfyorg/litegraph](https://www.npmjs.com/package/@comfyorg/litegraph)) renders the DAG.

- **Architecture split**: API routes under [app/api/](web-ui/my-app/app/api/) read/write `graph.json` on disk and spawn `prototype/graph_cli.py` for the node-type catalog. The editor itself lives in [components/GraphEditor.tsx](web-ui/my-app/components/GraphEditor.tsx). There is *no* separate Python web server — Next.js owns everything.
- **Named-port ↔ slot-index translation**: [lib/convert.ts](web-ui/my-app/lib/convert.ts). LiteGraph addresses slots by index; the on-disk graph addresses them by name. Conversion happens at load/save only — every other layer (API, file, Python pipeline) stays in port-name land.

### Next 16 and LiteGraph pitfalls

- **`params` is a `Promise`** in Next 15+. API and page handlers must `await ctx.params`.
- **LiteGraph must be `ssr: false`** — the library reads `document` / `window` at module init. [app/graph/page.tsx](web-ui/my-app/app/graph/page.tsx) uses `next/dynamic(() => import('@/components/GraphEditor'), { ssr: false })`.
- **Static `import "@comfyorg/litegraph/style.css"`**, not a runtime `await import()` — browsers don't execute CSS via the `import()` expression.
- **Custom node types must be ES6 classes extending `LGraphNode`**. `LiteGraph.registerNodeType` uses `for…in` over `LGraphNode.prototype` to mix in methods like `addInput` / `addOutput`, and ES6 class methods are non-enumerable — a plain `function Node() {}` silently ships nodes that crash on construction. See `makeNodeClass` in [lib/convert.ts](web-ui/my-app/lib/convert.ts).
- **`allowedDevOrigins: ["127.0.0.1", "localhost"]`** is set in [next.config.ts](web-ui/my-app/next.config.ts) — Next 16's default blocks cross-origin asset fetches and HMR, which bites Playwright/curl against `127.0.0.1`.
- **Next's own docs ship in `node_modules/next/dist/docs/`.** Read those rather than trusting training-data-era Next conventions.

### Test gotcha

`tests/smoke.mjs` back-ups and restores every `graph.json` around its run. If a run is killed mid-flight it may leave the `risk` module with 5 nodes instead of 4; run the smoke test once more (or `git checkout prototype/*/graph.json`) to restore.
