"""CLI used by the editor UI to introspect graph node schemas.

``python prototype/graph_cli.py schemas``
    Import every module's node catalog and print
    ``graph.all_node_schemas()`` as JSON.

Each module has its own ``config.py`` / ``snapshots.py`` living alongside
its ``main.py``; the module-scoped imports resolve via ``sys.path`` which we
rewrite per module. ``sys.modules`` is also purged of the conflicting names
between iterations so each module's ``from config import …`` picks up the
correct file.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

MODULE_DIRS: list[tuple[str, Path]] = [
    ("data", HERE / "data"),
    ("risk", HERE / "risk"),
    ("forecast", HERE / "forecast"),
    ("optimization", HERE / "optimization"),
]


def _load_all_modules() -> None:
    for pkg_name, pkg_dir in MODULE_DIRS:
        for conflict in ("config", "snapshots", "main"):
            sys.modules.pop(conflict, None)
        sys.path.insert(0, str(pkg_dir))
        try:
            spec = importlib.util.spec_from_file_location(
                f"{pkg_name}_main", pkg_dir / "main.py"
            )
            if spec is None or spec.loader is None:
                raise RuntimeError(f"could not load {pkg_dir / 'main.py'}")
            mod = importlib.util.module_from_spec(spec)
            # SQLAlchemy's declarative base resolves ``Mapped[T]`` annotations by
            # looking up ``sys.modules[module_name]`` at class-creation time, so
            # the module must be registered before exec_module runs.
            sys.modules[spec.name] = mod
            spec.loader.exec_module(mod)
        finally:
            sys.path.pop(0)


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: graph_cli.py <schemas>", file=sys.stderr)
        sys.exit(2)
    cmd = sys.argv[1]
    _load_all_modules()
    import graph

    if cmd == "schemas":
        print(json.dumps(graph.all_node_schemas(), indent=2, default=str))
    else:
        print(f"unknown command: {cmd}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
