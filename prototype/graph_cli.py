"""CLI used by the editor UI to introspect and validate the unified graph."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import graph
from node_loader import load_all_node_modules

HERE = Path(__file__).resolve().parent
DEFAULT_GRAPH_PATH = HERE / "graph.json"


def _read_graph_payload(path_arg: str | None) -> dict:
    if path_arg in (None, "-"):
        return json.load(sys.stdin)
    return json.loads(Path(path_arg).read_text())


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: graph_cli.py <schemas|validate> [path|-]", file=sys.stderr)
        sys.exit(2)

    load_all_node_modules()
    cmd = sys.argv[1]

    if cmd == "schemas":
        print(json.dumps(graph.all_node_schemas(), indent=2, default=str))
        return

    if cmd == "validate":
        path_arg = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_GRAPH_PATH)
        try:
            loaded = graph.load_graph_data(_read_graph_payload(path_arg))
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            sys.exit(1)
        summary = {
            "nodes": len(loaded.nodes),
            "edges": len(loaded.edges),
            "order": graph.topo_sort(loaded),
        }
        print(json.dumps(summary, indent=2))
        return

    print(f"unknown command: {cmd}", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
