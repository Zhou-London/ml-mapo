# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

ML-MAPO (Machine Learning Multi-Asset Portfolio Optimizer) is a research prototype by Philip Trealeaven's group at UCL CS. Two stacks live side by side:

- [prototype/](prototype/) — the Python pipeline, primary workload.
- [web-ui/my-app/](web-ui/my-app/) — a Next.js 16 + React 19 editor that reads and writes [prototype/graph.json](prototype/graph.json).

## Commands

Python toolchain is `uv` (requires Python ≥3.14 per [pyproject.toml](pyproject.toml)). Either activate `.venv` or prefix commands with `uv run`.

- `uv sync` — install/update Python deps.
- `uv run python prototype/main.py` — run the unified graph forever (Ctrl+C to stop). `--ticks N` runs N ticks then exits; `--graph PATH` points at a different graph file.
- `uv run python prototype/graph_cli.py schemas` — dump every `@register_node`-decorated class as JSON (palette catalog / debugging).
- `PYTHONUNBUFFERED=1 uv run python -u prototype/main.py 2>&1 | tee run.log` — capture logs when stdout is piped (child output is otherwise block-buffered).
- `cd web-ui/my-app && npm install && npm run dev` — Next.js editor (http://localhost:3000/graph).
- `cd web-ui/my-app && npm run build` — production build (also runs `tsc`).
- `cd web-ui/my-app && npm run lint` — ESLint via `next lint`.
- `cd web-ui/my-app && PORT=3100 node tests/smoke.mjs` — headless-Chromium end-to-end test against an already-running dev server on that port (requires `npx playwright install chromium` once). No Python test suite is wired up yet.

### Logging env vars ([prototype/_logging.py](prototype/_logging.py))
- `MAPO_LOG_LEVEL=DEBUG` — raise verbosity (default INFO).
- `MAPO_LOG_FORMAT=json` — newline-delimited JSON instead of the human-readable text format.
- `NO_COLOR=1` — disable ANSI colors.

### Database
TimescaleDB at `postgresql+psycopg2://postgres:password@localhost:6543/postgres` (default baked into the `data/Database` node in [prototype/graph.json](prototype/graph.json); override via the node's `url` param). The `ohlcv` table is promoted to a hypertable on `ts`.

## Pipeline architecture

See [doc/v0.1_architecture.md](doc/v0.1_architecture.md) for the v0.1 spec — note that spec still describes the old four-process ZeroMQ topology; the implementation has since collapsed into a single in-process graph.

**Topology** — one Python process runs [prototype/graph.py](prototype/graph.py)'s `Executor` over a single unified DAG defined in [prototype/graph.json](prototype/graph.json). No subprocesses, no sockets, no pickling — outputs flow directly as Python objects (pandas DataFrames/Series) along in-memory edges.

- [prototype/main.py](prototype/main.py) is a thin loader: `load_all_node_modules()` → `load_graph()` → `Executor.setup()` → `tick()` loop.
- [prototype/node_loader.py](prototype/node_loader.py) imports each module's `main.py` once to populate the `@register_node` registry. Each per-module `main.py` (`data/`, `risk/`, `forecast/`, `optimization/`) now contains *only* `Node` subclass definitions — there is no standalone entry point per module.
- [prototype/backtest/main.py](prototype/backtest/main.py) is an empty scaffold; nothing to wire yet.

## Graph system (important)

- **Adding a node type**: subclass `graph.Node`, declare `INPUTS` / `OUTPUTS` / `PARAMS` / `CATEGORY` as class attributes, implement `process(**inputs) -> dict`, and decorate with `@register_node("<module>/TypeName")`. Optional `setup()` / `teardown()` lifecycle hooks. Per-tick metadata (`seq`, `t0`) is passed via `self.ctx`. If the file lives outside one of the directories listed in `node_loader.MODULE_DIRS`, add it there — otherwise the class never registers.
- **Changing behavior**: edit the node's `params` entry in [prototype/graph.json](prototype/graph.json), or its `Node` subclass if you need new ports. There is no imperative `main()` to patch per module.
- **On-disk format**: `{nodes: [{id, type, params, pos}], edges: [{src_node, src_port, dst_node, dst_port}]}`. Ports are referenced by name — the JSON is independent of the UI's slot ordering.
- **Default factor plugins**:
  - `RiskFactor` in [prototype/risk/main.py](prototype/risk/main.py) — default `naive_sample_cov` (annualized sample covariance of daily log returns on `adj_close`, lookback 252). Select via the `factor` param on `risk/Covariance`.
  - `AlphaFactor` in [prototype/forecast/main.py](prototype/forecast/main.py) — default `momentum_12_1`. The `factors` / `information_ratios` params on `forecast/Alpha` accept CSVs; multiple factors combine via IR-weighted z-score rescaling.
  - Optimization: `opt/Optimizer` runs `scipy.optimize.minimize(SLSQP)` with an analytic gradient; defaults `risk_aversion=50`, `long_only=true`.

## Logging

Every module goes through [prototype/_logging.py](prototype/_logging.py):

- `get_logger(module)` and `run_module(module, main_fn)` — the latter wraps `__main__` and handles crash/interrupt reporting.
- `log.pipeline("stage", **fields)` context manager — stacks stage names, emits start/done/failed with duration.
- `log.snapshot(name, data)` — structured state sample (dashboard panel in JSON mode; compact summary in text mode).
- `log.table(msg, rows, headers=...)` — indented aligned block.
- Each module publishes its snapshot schemas in `<module>/snapshots.py`.

## Web UI (Next.js 16 + LiteGraph)

[web-ui/my-app/](web-ui/my-app/) is an app-router Next.js 16 project that edits the single [prototype/graph.json](prototype/graph.json). ComfyUI's canvas library ([@comfyorg/litegraph](https://www.npmjs.com/package/@comfyorg/litegraph)) renders the DAG.

- **Architecture split**: API routes under [app/api/graph/](web-ui/my-app/app/api/graph/) read/write `graph.json` on disk via [app/api/graph/_lib/graph-store.ts](web-ui/my-app/app/api/graph/_lib/graph-store.ts), which shells out to `prototype/graph_cli.py` for the node-type catalog and for `POST /api/graph/run` (launches the pipeline for N ticks). The editor itself lives in [components/GraphEditor.tsx](web-ui/my-app/components/GraphEditor.tsx). There is *no* separate Python web server — Next.js owns everything.
- **Named-port ↔ slot-index translation**: [lib/convert.ts](web-ui/my-app/lib/convert.ts). LiteGraph addresses slots by index; the on-disk graph addresses them by name. Conversion happens at load/save only — every other layer (API, file, Python pipeline) stays in port-name land.

### Next 16 and LiteGraph pitfalls

- **`params` is a `Promise`** in Next 15+. API and page handlers must `await ctx.params`.
- **LiteGraph must be `ssr: false`** — the library reads `document` / `window` at module init. [app/graph/page.tsx](web-ui/my-app/app/graph/page.tsx) uses `next/dynamic(() => import('@/components/GraphEditor'), { ssr: false })`.
- **Static `import "@comfyorg/litegraph/style.css"`**, not a runtime `await import()` — browsers don't execute CSS via the `import()` expression.
- **Custom node types must be ES6 classes extending `LGraphNode`**. `LiteGraph.registerNodeType` uses `for…in` over `LGraphNode.prototype` to mix in methods like `addInput` / `addOutput`, and ES6 class methods are non-enumerable — a plain `function Node() {}` silently ships nodes that crash on construction. See `makeNodeClass` in [lib/convert.ts](web-ui/my-app/lib/convert.ts).
- **`allowedDevOrigins: ["127.0.0.1", "localhost"]`** is set in [next.config.ts](web-ui/my-app/next.config.ts) — Next 16's default blocks cross-origin asset fetches and HMR, which bites Playwright/curl against `127.0.0.1`.
- **Next's own docs ship in `node_modules/next/dist/docs/`.** Read those rather than trusting training-data-era Next conventions.

### Test gotcha

`tests/smoke.mjs` backs up and restores [prototype/graph.json](prototype/graph.json) around its run. If a run is killed mid-flight it may leave the file modified; `git checkout prototype/graph.json` restores it.
