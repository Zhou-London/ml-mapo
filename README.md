# ml-mapo

Machine Learning Multi-Asset Portfolio Optimizer (ML-MAPO)  
Philip Treleaven, UCL Computer Science

## Dependencies

- Linux
- TimescaleDB
- Python via `uv` (Python >= 3.14 per [pyproject.toml](pyproject.toml))
- Node >= 20 for the editor under [web-ui/my-app/](web-ui/my-app/)
- Docker (optional, for the DB)

## Setup

Install Python dependencies:

```zsh
uv sync
```

Activate the virtual environment if you want plain `python`:

```zsh
source .venv/bin/activate
```

Or run Python commands through `uv`:

```zsh
uv run python --version
```

Start TimescaleDB, for example with Docker:

```zsh
docker run -d --name timescaledb \
  -p 6543:5432 \
  -e POSTGRES_PASSWORD=password \
  timescale/timescaledb-ha:pg18

psql -h localhost -p 6543 -U postgres
```

## Run the Prototype

Run the unified graph forever:

```zsh
uv run python prototype/main.py
```

Run exactly one tick, which is useful for UI-triggered execution and debugging:

```zsh
uv run python prototype/main.py --ticks 1
```

Validate the persisted graph without executing it:

```zsh
uv run python prototype/graph_cli.py validate prototype/graph.json
```

## Graph Runtime

The pipeline is now one DAG stored in [prototype/graph.json](prototype/graph.json).  
There is no ZeroMQ layer and no multi-process supervisor anymore.

- Runtime core: [prototype/graph.py](prototype/graph.py)
- Unified entry point: [prototype/main.py](prototype/main.py)
- Node catalog loader: [prototype/node_loader.py](prototype/node_loader.py)
- Persisted graph: [prototype/graph.json](prototype/graph.json)

Node classes still live in the module folders:

- [prototype/data/main.py](prototype/data/main.py)
- [prototype/risk/main.py](prototype/risk/main.py)
- [prototype/forecast/main.py](prototype/forecast/main.py)
- [prototype/optimization/main.py](prototype/optimization/main.py)

Those files are now node catalogs, not standalone pipeline runners.

### Node catalog

| Type                  | Inputs                      | Output          | Notes |
| --------------------- | --------------------------- | --------------- | ----- |
| `data/DateRange`      | —                           | `start`, `end`  | Blank date params fall back to a 365-day window ending today. |
| `data/Database`       | —                           | `engine`        | Owns the SQLAlchemy engine and the `ohlcv` hypertable. |
| `data/USEquity`       | `engine`, `start`, `end`    | `frame`         | US equities (yfinance). |
| `data/UKEquity`       | `engine`, `start`, `end`    | `frame`         | LSE equities (yfinance, `.L` suffix). |
| `data/FX`             | `engine`, `start`, `end`    | `frame`         | FX pairs (yfinance, `=X` suffix). |
| `data/Aggregate`      | `a`, `b` (`frame`)          | `frame`         | Column-wise concat; chain for ≥ 3 asset classes. |
| `risk/Covariance`     | `frame`                     | `cov`           | Defaults to annualized sample covariance (`naive_sample_cov`). |
| `forecast/Alpha`      | `frame`                     | `alpha`         | Defaults to 12-1 momentum; IR-weighted z-score blending across factors. |
| `opt/Optimizer`       | `cov`, `alpha`              | `weights`       | SLSQP mean-variance, long-only by default. |
| `opt/WeightsDisplay`  | `weights`                   | `text`          | Pretty-prints the weights to stdout (visible in the UI run console). |

The `frame` port type is a wide `pandas.DataFrame` indexed by date with tickers as columns (values are `adj_close`). Asset-class nodes upsert missing bars from yfinance into TimescaleDB before returning.

### Validation rules

The runtime rejects illegal graphs before execution:

- unknown node types
- duplicate node ids
- edges pointing at missing nodes
- unknown input/output port names
- type mismatches across edges
- multiple edges feeding the same input port
- missing required inputs
- cycles

Execution order is strict across the whole persisted DAG.  
`topo_sort()` computes one global order and every tick executes nodes in that order.

## Add a Node Type

1. Subclass `graph.Node` in the appropriate module catalog.
2. Declare `INPUTS`, `OUTPUTS`, `PARAMS`, and `CATEGORY`.
3. Implement `process(**inputs) -> dict`.
4. Decorate it with `@register_node("<module>/YourNode")`.
5. Add it to [prototype/graph.json](prototype/graph.json) directly or through the editor.
6. Refresh the schema cache with `GET /api/graph/schemas?refresh=1` if needed.

The on-disk graph format is:

```json
{
  "nodes": [{ "id": "node_id", "type": "module/Type", "params": {}, "pos": [0, 0] }],
  "edges": [{ "src_node": "a", "src_port": "out", "dst_node": "b", "dst_port": "in" }]
}
```

Ports are named, so the saved graph is independent of LiteGraph slot order.

## Web UI

The editor lives in [web-ui/my-app/](web-ui/my-app/) and is a Next.js 16 + React 19 app using [@comfyorg/litegraph](https://www.npmjs.com/package/@comfyorg/litegraph).

Start it with:

```zsh
cd web-ui/my-app
npm install
npm run dev
```

Then open [http://localhost:3000/graph](http://localhost:3000/graph).

### What the UI does

- loads schemas from `prototype/graph_cli.py`
- reads and writes [prototype/graph.json](prototype/graph.json)
- validates the graph on save using the Python runtime rules
- lets the user launch one execution tick directly from the UI
- shows runtime or validation errors in the status bar and run output panel

Current editor features:

- searchable node palette
- adjustable sidebar
- dark/light mode
- custom canvas background and grid
- category-colored nodes on the canvas
- explicitly colored wires plus distinct input-port and output-port colors
- reduced Mac trackpad pinch sensitivity
- no LiteGraph canvas border

The canvas is intentionally not monochrome. Node colors are applied by module
category (`data`, `forecast`, `risk`, `opt`) after the graph loads, and the
editor also stamps the link color onto the live LiteGraph links so the canvas
stays readable in both themes.

![editor screenshot](web-ui/my-app/tests/screenshot.png)

### API endpoints

- `GET /graph` — editor page
- `GET /api/graph` — read the unified graph
- `PUT /api/graph` — validate and overwrite the unified graph
- `POST /api/graph/run` — save-and-run flow target; executes one tick
- `GET /api/graph/schemas[?refresh=1]` — refreshable node schema catalog

The Next app is still just the editor-side bridge.  
The Python runtime executes the graph; there is no separate Python web server.

### LiteGraph note

Named-port to slot-index translation lives in [web-ui/my-app/lib/convert.ts](web-ui/my-app/lib/convert.ts).  
Only the canvas layer uses slot indices; the runtime and saved graph stay in named-port form.

Coloring is also slightly non-obvious with LiteGraph: canvas-level defaults are
not enough for the rendered nodes and persisted links in this editor. Theme
changes explicitly recolor node instances and live links in
[web-ui/my-app/components/GraphEditor.tsx](web-ui/my-app/components/GraphEditor.tsx).

## Tests

Build the editor:

```zsh
cd web-ui/my-app
npm run build
```

Run the browser smoke test:

```zsh
cd web-ui/my-app
npx playwright install chromium
PORT=3000 node tests/smoke.mjs
```

`tests/smoke.mjs` restores [prototype/graph.json](prototype/graph.json) after it runs.

## Historical Architecture Doc

[doc/v0.1_architecture.md](doc/v0.1_architecture.md) describes the older multi-process design and is no longer the source of truth for runtime topology.
