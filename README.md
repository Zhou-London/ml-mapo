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