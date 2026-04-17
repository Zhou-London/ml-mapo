"""Graph runtime for the ML-MAPO prototype.

Each pipeline module is a DAG of ``Node`` instances. This module provides:

- ``Node`` — base class: declare ``INPUTS`` / ``OUTPUTS`` / ``PARAMS`` as class
  attributes, implement ``process(**inputs) -> dict``. ``setup()`` is called
  once before the first tick; ``teardown()`` once after the last.
- ``register_node(type_name)`` — decorator that adds a node class to the
  global registry. The type name is also the identifier used in graph JSON.
- ``Graph`` / ``Edge`` — serializable topology.
- ``Executor`` — topologically sorts a graph, calls ``setup()`` once, then
  runs ``tick()`` in a loop, routing outputs along edges as inputs.
- ``load_graph`` / ``save_graph`` — on-disk JSON format consumed by both the
  runtime and the editor UI.

The JSON format is intentionally ``litegraph.js``-agnostic: ports are named,
not positional, so refactoring a node's slot order doesn't corrupt saved
graphs. The UI layer translates between litegraph's slot indices and these
port names at load/save time.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, ClassVar


# ---------- Node base + registry ----------


class Node:
    """Base class for graph nodes.

    Subclasses declare the port schema via class-level constants and
    implement ``process``. A thin layer over plain classes: the graph system
    does not care about anything except the schema and the three lifecycle
    hooks.
    """

    # Free-form type tags per port. The executor does no type checking; the
    # labels are used by the UI palette and for human readability.
    INPUTS: ClassVar[dict[str, str]] = {}
    OUTPUTS: ClassVar[dict[str, str]] = {}
    # {name: (type_hint, default_value)}
    PARAMS: ClassVar[dict[str, tuple[str, Any]]] = {}

    CATEGORY: ClassVar[str] = "general"
    TYPE_NAME: ClassVar[str] = ""  # set by @register_node

    def __init__(self, node_id: str, params: dict[str, Any] | None = None) -> None:
        self.node_id = node_id
        defaults = {name: spec[1] for name, spec in self.PARAMS.items()}
        self.params: dict[str, Any] = {**defaults, **(params or {})}
        # Populated by the executor right before process() so nodes can read
        # per-tick metadata (seq, t0) without declaring it as an input port.
        self.ctx: dict[str, Any] = {}

    def setup(self) -> None:
        """Called once before the first tick. Default: nothing."""

    def process(self, **inputs: Any) -> dict[str, Any]:
        """Run one tick. Return a dict keyed by the names in ``OUTPUTS``."""
        raise NotImplementedError

    def teardown(self) -> None:
        """Called once after the executor stops. Default: nothing."""


_REGISTRY: dict[str, type[Node]] = {}


def register_node(type_name: str) -> Callable[[type[Node]], type[Node]]:
    """Class decorator that registers a node under ``type_name``.

    The type name is namespaced by module (e.g. ``data/FetchStore``) so
    node-palette filtering in the UI stays tractable as modules grow.
    """

    def decorate(cls: type[Node]) -> type[Node]:
        if type_name in _REGISTRY:
            raise ValueError(f"node type already registered: {type_name!r}")
        cls.TYPE_NAME = type_name
        _REGISTRY[type_name] = cls
        return cls

    return decorate


def get_node_class(type_name: str) -> type[Node]:
    try:
        return _REGISTRY[type_name]
    except KeyError as e:
        raise KeyError(
            f"unknown node type {type_name!r}; registered: {sorted(_REGISTRY)}"
        ) from e


def all_node_schemas() -> list[dict[str, Any]]:
    """Dump every registered node's schema for the editor palette."""
    schemas: list[dict[str, Any]] = []
    for type_name, cls in sorted(_REGISTRY.items()):
        schemas.append(
            {
                "type": type_name,
                "category": cls.CATEGORY,
                "doc": (cls.__doc__ or "").strip().splitlines()[0] if cls.__doc__ else "",
                "inputs": [{"name": k, "type": v} for k, v in cls.INPUTS.items()],
                "outputs": [{"name": k, "type": v} for k, v in cls.OUTPUTS.items()],
                "params": [
                    {"name": k, "type": spec[0], "default": spec[1]}
                    for k, spec in cls.PARAMS.items()
                ],
            }
        )
    return schemas


# ---------- Graph + Executor ----------


@dataclass
class Edge:
    """Directed edge: ``(src_node, src_port) → (dst_node, dst_port)``."""

    src_node: str
    src_port: str
    dst_node: str
    dst_port: str


@dataclass
class NodeSpec:
    """Serializable form of a node: the runtime ``Node`` instance plus UI layout."""

    id: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)
    pos: list[float] = field(default_factory=lambda: [0.0, 0.0])


@dataclass
class Graph:
    """Runtime graph: instantiated Node objects + edges + UI specs."""

    nodes: dict[str, Node]
    edges: list[Edge]
    specs: dict[str, NodeSpec]


class Executor:
    """Runs a ``Graph`` in topological order, tick by tick."""

    def __init__(self, graph: Graph) -> None:
        self.graph = graph
        self.ctx: dict[str, Any] = {"seq": 0}
        self._order = self._topo_sort()
        self._inputs_for: dict[str, list[Edge]] = defaultdict(list)
        for e in graph.edges:
            self._inputs_for[e.dst_node].append(e)

    def _topo_sort(self) -> list[str]:
        incoming: dict[str, set[str]] = {n: set() for n in self.graph.nodes}
        outgoing: dict[str, set[str]] = {n: set() for n in self.graph.nodes}
        for e in self.graph.edges:
            if e.src_node not in self.graph.nodes or e.dst_node not in self.graph.nodes:
                raise ValueError(
                    f"edge references unknown node: {e.src_node} -> {e.dst_node}"
                )
            incoming[e.dst_node].add(e.src_node)
            outgoing[e.src_node].add(e.dst_node)
        order: list[str] = []
        ready = sorted(n for n, ins in incoming.items() if not ins)
        while ready:
            n = ready.pop(0)
            order.append(n)
            for m in sorted(outgoing[n]):
                incoming[m].discard(n)
                if not incoming[m]:
                    ready.append(m)
        if len(order) != len(self.graph.nodes):
            missing = set(self.graph.nodes) - set(order)
            raise ValueError(f"graph has a cycle; could not order: {sorted(missing)}")
        return order

    def setup(self) -> None:
        for nid in self._order:
            self.graph.nodes[nid].setup()

    def tick(self) -> dict[str, dict[str, Any]]:
        """Run one full pass through the graph; returns {node_id: outputs}."""
        self.ctx["seq"] += 1
        outputs: dict[str, dict[str, Any]] = {}
        for nid in self._order:
            node = self.graph.nodes[nid]
            node.ctx = self.ctx
            kwargs: dict[str, Any] = {}
            for e in self._inputs_for[nid]:
                src_out = outputs.get(e.src_node) or {}
                if e.src_port not in src_out:
                    raise KeyError(
                        f"upstream {e.src_node}.{e.src_port} did not produce a value"
                        f" for {nid}.{e.dst_port}"
                    )
                kwargs[e.dst_port] = src_out[e.src_port]
            result = node.process(**kwargs) or {}
            outputs[nid] = result
        return outputs

    def teardown(self) -> None:
        for nid in reversed(self._order):
            try:
                self.graph.nodes[nid].teardown()
            except Exception:  # teardown must not mask shutdown progress
                pass


# ---------- JSON I/O ----------


def load_graph(path: str | Path) -> Graph:
    """Instantiate a graph from a JSON file.

    Unknown node types raise immediately so typos surface at startup instead
    of mid-run.
    """
    data = json.loads(Path(path).read_text())
    specs: dict[str, NodeSpec] = {}
    nodes: dict[str, Node] = {}
    for raw in data["nodes"]:
        spec = NodeSpec(
            id=str(raw["id"]),
            type=raw["type"],
            params=dict(raw.get("params") or {}),
            pos=list(raw.get("pos") or [0.0, 0.0]),
        )
        cls = get_node_class(spec.type)
        specs[spec.id] = spec
        nodes[spec.id] = cls(node_id=spec.id, params=spec.params)
    edges = [
        Edge(
            src_node=str(e["src_node"]),
            src_port=e["src_port"],
            dst_node=str(e["dst_node"]),
            dst_port=e["dst_port"],
        )
        for e in data.get("edges", [])
    ]
    return Graph(nodes=nodes, edges=edges, specs=specs)


def save_graph(graph: Graph, path: str | Path) -> None:
    """Write the graph to disk in the canonical JSON format."""
    data = {
        "nodes": [asdict(graph.specs[nid]) for nid in graph.specs],
        "edges": [asdict(e) for e in graph.edges],
    }
    Path(path).write_text(json.dumps(data, indent=2))


def graph_to_dict(graph: Graph) -> dict[str, Any]:
    """Dict form used by the editor HTTP API (no on-disk side effect)."""
    return {
        "nodes": [asdict(graph.specs[nid]) for nid in graph.specs],
        "edges": [asdict(e) for e in graph.edges],
    }
