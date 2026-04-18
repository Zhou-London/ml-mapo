# AGENTS.md

This file provides guidance to Codex when working in this repository.

ML-MAPO now has two active stacks:

- [prototype/](prototype/) — the Python runtime and node catalogs.
- [web-ui/my-app/](web-ui/my-app/) — the Next.js editor for the unified graph.

## Commands

Use `uv` for Python work.

- `uv sync` — install Python dependencies.
- `uv run python prototype/main.py` — run the unified graph forever.
- `uv run python prototype/main.py --ticks 1` — run one tick and exit.
- `uv run python prototype/graph_cli.py schemas` — dump registered node schemas as JSON.
- `uv run python prototype/graph_cli.py validate prototype/graph.json` — validate the persisted graph.
- `cd web-ui/my-app && npm install && npm run dev` — start the editor on `http://localhost:3000/graph`.
- `cd web-ui/my-app && npm run build` — production build and TypeScript check.
- `cd web-ui/my-app && PORT=3000 node tests/smoke.mjs` — browser smoke test.

## Database

TimescaleDB is hard-coded at:

- `postgresql+psycopg2://postgres:password@localhost:6543/postgres`

See [prototype/data/config.py](prototype/data/config.py).

## Runtime Architecture

The old ZeroMQ multi-process pipeline is gone.

- There is one persisted DAG: [prototype/graph.json](prototype/graph.json)
- There is one runtime entry point: [prototype/main.py](prototype/main.py)
- Node types are imported by [prototype/node_loader.py](prototype/node_loader.py)
- Node definitions still live in the module folders under `prototype/*/main.py`

Treat `prototype/data/main.py`, `prototype/risk/main.py`, `prototype/forecast/main.py`, and `prototype/optimization/main.py` as node catalogs, not standalone runners.

### Validation guarantees

The Python runtime rejects:

- duplicate node ids
- edges to missing nodes
- unknown ports
- type mismatches
- multiple sources feeding one input
- missing required inputs
- cycles

Execution order is strict across the entire graph, not per-module.  
`topo_sort()` computes one global order and `Executor.tick()` runs nodes in that order.

## Graph System

Core runtime: [prototype/graph.py](prototype/graph.py)

Saved graph format:

```json
{
  "nodes": [{ "id": "node_id", "type": "module/Type", "params": {}, "pos": [0, 0] }],
  "edges": [{ "src_node": "a", "src_port": "out", "dst_node": "b", "dst_port": "in" }]
}
```

Ports are referenced by name. The UI's slot indices are translated only at load/save boundaries in [web-ui/my-app/lib/convert.ts](web-ui/my-app/lib/convert.ts).

### Adding or changing nodes

- Add a node by subclassing `graph.Node`, declaring `INPUTS` / `OUTPUTS` / `PARAMS` / `CATEGORY`, implementing `process`, and decorating with `@register_node("<module>/Type")`.
- Change runtime behavior by editing the node class or the node params in [prototype/graph.json](prototype/graph.json).

Default factor implementations:

- [prototype/risk/main.py](prototype/risk/main.py) — `risk/Covariance` defaults to `NaiveRiskFactor`
- [prototype/forecast/main.py](prototype/forecast/main.py) — `forecast/Alpha` defaults to `NaiveMomentumAlpha`
- [prototype/optimization/main.py](prototype/optimization/main.py) — `opt/Optimizer` uses SLSQP with analytic gradient

## Web UI

The editor is a Next.js 16 app-router project under [web-ui/my-app/](web-ui/my-app/).

- `GET /api/graph` reads [prototype/graph.json](prototype/graph.json)
- `PUT /api/graph` validates and writes it
- `POST /api/graph/run` executes one tick and returns stdout/stderr
- `GET /api/graph/schemas` returns the node catalog

Important files:

- [web-ui/my-app/components/GraphEditor.tsx](web-ui/my-app/components/GraphEditor.tsx)
- [web-ui/my-app/app/api/graph/_lib/graph-store.ts](web-ui/my-app/app/api/graph/_lib/graph-store.ts)
- [web-ui/my-app/lib/convert.ts](web-ui/my-app/lib/convert.ts)

Current UI affordances:

- searchable unified node palette
- adjustable sidebar
- light/dark mode
- custom canvas background
- category-colored nodes
- differentiated wire/input/output colors
- reduced Mac touchpad zoom sensitivity
- run button with validation/runtime error reporting

### Next/LiteGraph pitfalls

- API/page handlers in Next 15+ treat `params` as a promise.
- LiteGraph must stay `ssr: false`.
- Import `@comfyorg/litegraph/style.css` statically.
- Custom LiteGraph nodes must be ES6 classes extending `LGraphNode`.
- Keep `allowedDevOrigins` in [web-ui/my-app/next.config.ts](web-ui/my-app/next.config.ts) for local browser automation.
- When adding widgets in `convert.ts`, bind them to a property or callback or LiteGraph logs `addWidget(...) without a callback or property assigned`.
- Theme defaults alone do not reliably color existing nodes and links. If canvas colors matter, update the live graph objects in [web-ui/my-app/components/GraphEditor.tsx](web-ui/my-app/components/GraphEditor.tsx), not just LiteGraph globals.

## Tests

`web-ui/my-app/tests/smoke.mjs` drives the unified editor through load, palette add, save, and reload. It restores [prototype/graph.json](prototype/graph.json) at the end.

## Historical note

[doc/v0.1_architecture.md](doc/v0.1_architecture.md) documents the older ZeroMQ design and is now historical context, not the current runtime contract.
