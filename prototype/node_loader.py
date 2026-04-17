"""Helpers for importing every node catalog into the shared graph registry."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODULE_DIRS: list[tuple[str, Path]] = [
    ("data", HERE / "data"),
    ("risk", HERE / "risk"),
    ("forecast", HERE / "forecast"),
    ("optimization", HERE / "optimization"),
]

_LOADED = False


def load_all_node_modules() -> None:
    """Import each module's node catalog exactly once."""
    global _LOADED
    if _LOADED:
        return

    sys.path.insert(0, str(HERE))
    try:
        for pkg_name, pkg_dir in MODULE_DIRS:
            for conflict in ("config", "snapshots", "main"):
                sys.modules.pop(conflict, None)
            sys.path.insert(0, str(pkg_dir))
            try:
                spec = importlib.util.spec_from_file_location(
                    f"{pkg_name}_nodes", pkg_dir / "main.py"
                )
                if spec is None or spec.loader is None:
                    raise RuntimeError(f"could not load {pkg_dir / 'main.py'}")
                mod = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = mod
                spec.loader.exec_module(mod)
            finally:
                sys.path.pop(0)
    finally:
        sys.path.pop(0)

    _LOADED = True
