"""Single-process entry point for the unified ML-MAPO graph."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from _logging import get_logger, run_module
from graph import Executor, load_graph
from node_loader import load_all_node_modules

DEFAULT_GRAPH_PATH = Path(__file__).resolve().parent / "graph.json"

log = get_logger("runner")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--graph",
        default=str(DEFAULT_GRAPH_PATH),
        help="path to the unified graph JSON",
    )
    parser.add_argument(
        "--ticks",
        type=int,
        default=0,
        help="run this many ticks then exit; 0 means run forever",
    )
    return parser.parse_args()


def run_graph(graph_path: Path, ticks: int) -> None:
    load_all_node_modules()

    graph = load_graph(graph_path)
    executor = Executor(graph)
    log.info(
        "graph loaded",
        path=str(graph_path),
        nodes=len(graph.nodes),
        edges=len(graph.edges),
    )
    executor.setup()

    try:
        if ticks > 0:
            for _ in range(ticks):
                executor.ctx["t0"] = time.monotonic()
                executor.tick()
        else:
            while True:
                executor.ctx["t0"] = time.monotonic()
                executor.tick()
    except KeyboardInterrupt:
        pass
    finally:
        log.info("closing", ticks=int(executor.ctx.get("seq", 0)))
        executor.teardown()


def main() -> None:
    args = parse_args()
    run_graph(Path(args.graph), ticks=int(args.ticks))


if __name__ == "__main__":
    run_module("runner", main)
