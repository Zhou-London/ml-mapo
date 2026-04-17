# ml-mapo
Machine Learning Multi-Asset Portfolio Optimizer (ML-MAPO)

@Philip Trealeaven, UCL Computer Science

## Dependencies
- Linux
- TimescaleDB
- Python (uv)
- Docker
- Node

## Build
Activate the Python uv environment:
```zsh
$ source .venv/bin/activate
```

(Optional) Alternatively, use `uv run` instead of `python`:
```zsh
$ uv run python main.py
``` 

Install the python dependencies using `uv`. This will install packages including pytorch, pandas, etc.
```zsh
$ uv sync
```

Install PostgresSQL and TimescaleDB following the tutorial [here](https://github.com/timescale/timescaledb). Connect to the database:

```zsh
$ psql -h localhost -p 6543 -U postgres
```

(Optional) If use Docker, reconnect to the db.

```zsh
$ docker ps -a
$ docker start <your-db-service>
```

Install the web-ui dependency.

```zsh
$ cd web-ui/my-app
$ npm install
```

Run the web-ui.

```zsh
$ npm run dev
```

(Optional) Alternatively, run the prototype in CLI.
```zsh
$ uv run python prototype/main.py
```

(Debug) Run a dedicated module.
```zsh
$ uv run python prototype/data/main.py
```

## Graph pipeline

Each of the four prototype modules (`data`, `risk`, `forecast`, `optimization`)
is now a DAG of `Node` classes defined in its `main.py` and wired together by a
JSON blueprint beside it (`<module>/graph.json`). At startup the module loads
the graph, topologically sorts it, calls `setup()` once, and runs `tick()`
forever. ZMQ PUB/SUB still bridges modules; the graph only describes what
happens *inside* one module per tick.

- Runtime: [prototype/graph.py](prototype/graph.py)
- Blueprints: [data/graph.json](prototype/data/graph.json),
  [risk/graph.json](prototype/risk/graph.json),
  [forecast/graph.json](prototype/forecast/graph.json),
  [optimization/graph.json](prototype/optimization/graph.json)

### Run the graph pipeline

Exactly the same as the non-graph run — the supervisor spawns each module,
which loads its own `graph.json`:

```zsh
$ uv run python prototype/main.py
```

You'll see a `graph loaded` line per module on startup, e.g.
`graph loaded path=…/data/graph.json nodes=6`.

To run a single module standalone (handy for debugging one graph in
isolation; downstream modules block on `recv` until upstream publishes):

```zsh
$ uv run python prototype/risk/main.py
```

### Edit graphs in the browser

The web UI ships a ComfyUI-style blueprint editor at `/graph`:

```zsh
$ cd web-ui/my-app && npm run dev
# then open http://localhost:3000/graph
```

Pick a module from the tabs (Data / Risk / Forecast / Optimization), drag
nodes, wire ports, edit parameters in the node widgets, hit **Save**. The
editor writes the module's `graph.json` back to disk. Restart the pipeline
(or the single module) for changes to take effect.

Under the hood:
- `GET /api/graph/schemas` — spawns [prototype/graph_cli.py](prototype/graph_cli.py)
  which imports every module to collect `@register_node`-decorated classes,
  then returns their input/output/param schemas. Cached in memory; hit
  `?refresh=1` after changing node source.
- `GET /api/graph/<module>` / `PUT /api/graph/<module>` — read/write the
  module's `graph.json`.

### Add a new node type

1. Subclass `graph.Node` inside the module's `main.py`, declare
   `INPUTS` / `OUTPUTS` / `PARAMS` / `CATEGORY`, implement `process(**inputs)`
   (and optionally `setup` / `teardown`), and decorate with
   `@register_node("<module>/YourNode")`.
2. Add an entry to the module's `graph.json` (or drop the node in the editor
   and hit Save).
3. If the editor is running, hit `GET /api/graph/schemas?refresh=1` so the
   palette picks up the new type.