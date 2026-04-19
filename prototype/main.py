"""Single-process entry point for the unified ML-MAPO graph."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from graph import Executor, load_graph
from node_loader import load_all_node_modules

DEFAULT_GRAPH_PATH = Path(__file__).resolve().parent / "graph.json"

NODE_EVENT_PREFIX = "__NODE_EVENT__ "


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
    parser.add_argument(
        "--emit-events",
        action="store_true",
        help="print per-node start/end events as JSON lines on stdout",
    )
    return parser.parse_args()


def _print_event(event: dict[str, Any]) -> None:
    sys.stdout.write(NODE_EVENT_PREFIX + json.dumps(event) + "\n")
    sys.stdout.flush()


def run_graph(graph_path: Path, ticks: int, emit_events: bool = False) -> None:
    load_all_node_modules()

    graph = load_graph(graph_path)
    executor = Executor(graph)
    if emit_events:
        executor.on_event = _print_event  # consumed by Executor when supported
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
        executor.teardown()


def main() -> None:
    args = parse_args()
    run_graph(Path(args.graph), ticks=int(args.ticks), emit_events=args.emit_events)


if __name__ == "__main__":
    main()
