import {
  LGraphNode,
  LiteGraph,
  type LGraph,
} from "@comfyorg/litegraph";

import type {
  GraphDoc,
  GraphEdge,
  GraphNode,
  NodeSchema,
} from "./types";

// Stash the original schema on the node so saveFromGraph can recover the
// canonical input/output port names (LiteGraph may mutate its own slot list).
const SCHEMA_KEY = "__mapo_schema";
// Preserve the original node id from the doc across save/load round-trips.
const NODE_ID_KEY = "__mapo_id";

type MapoLGraphNode = LGraphNode & {
  [SCHEMA_KEY]?: NodeSchema;
  [NODE_ID_KEY]?: string;
};

export function registerNodeTypes(schemas: NodeSchema[]): void {
  for (const schema of schemas) {
    const ctor = buildNodeClass(schema);
    // LiteGraph errors if a type is already registered; unregister first so
    // hot-reload and repeated `fetchSchemas()` calls stay idempotent.
    try {
      LiteGraph.unregisterNodeType(schema.type);
    } catch {
      /* not registered yet */
    }
    LiteGraph.registerNodeType(schema.type, ctor);
  }
}

function buildNodeClass(schema: NodeSchema): typeof LGraphNode {
  class MapoNode extends LGraphNode {
    static title = schema.type.split("/").slice(1).join("/") || schema.type;

    constructor(title?: string) {
      super(title ?? MapoNode.title);
      (this as MapoLGraphNode)[SCHEMA_KEY] = schema;
      for (const input of schema.inputs) {
        this.addInput(input.name, input.type);
      }
      for (const output of schema.outputs) {
        this.addOutput(output.name, output.type);
      }
      for (const param of schema.params) {
        const def = param.default;
        const propertyBinding = param.name as unknown as never;
        if (typeof def === "number") {
          this.addWidget("number", param.name, def, propertyBinding);
        } else if (typeof def === "boolean") {
          this.addWidget("toggle", param.name, def, propertyBinding);
        } else {
          this.addWidget("text", param.name, def == null ? "" : String(def), propertyBinding);
        }
      }
    }
  }
  Object.defineProperty(MapoNode, "name", { value: `MapoNode_${schema.type}` });
  return MapoNode;
}

export function loadIntoGraph(graph: LGraph, doc: GraphDoc): void {
  graph.clear();
  const byId = new Map<string, LGraphNode>();

  for (const n of doc.nodes) {
    const node = LiteGraph.createNode(n.type) as MapoLGraphNode | null;
    if (!node) {
      console.warn(`[convert] unknown node type, skipping: ${n.type}`);
      continue;
    }
    if (n.pos) node.pos = [n.pos[0], n.pos[1]];
    node[NODE_ID_KEY] = n.id;

    // Apply param values onto matching widgets.
    if (n.params && node.widgets) {
      for (const w of node.widgets) {
        if (w.name in n.params) {
          const v = n.params[w.name as string];
          (w as { value: unknown }).value = v as never;
        }
      }
    }
    graph.add(node);
    byId.set(n.id, node);
  }

  for (const e of doc.edges) {
    const src = byId.get(e.src_node);
    const dst = byId.get(e.dst_node);
    if (!src || !dst) {
      console.warn(`[convert] edge references missing node: ${e.src_node} -> ${e.dst_node}`);
      continue;
    }
    const srcSlot = src.findOutputSlot(e.src_port, false);
    const dstSlot = dst.findInputSlot(e.dst_port, false);
    if (srcSlot < 0 || dstSlot < 0) {
      console.warn(
        `[convert] edge port not found: ${e.src_node}.${e.src_port} -> ${e.dst_node}.${e.dst_port}`,
      );
      continue;
    }
    src.connect(srcSlot, dst, dstSlot);
  }
}

export function saveFromGraph(graph: LGraph): GraphDoc {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  const nodeList = (graph._nodes ?? []) as MapoLGraphNode[];
  const idOf = new Map<number, string>();
  const seen = new Set<string>();

  for (const node of nodeList) {
    const preferred = node[NODE_ID_KEY];
    const id = uniqueId(preferred ?? shortId(node.type), seen);
    seen.add(id);
    idOf.set(node.id as number, id);
    // Persist the resolved id back so future round-trips stay stable.
    node[NODE_ID_KEY] = id;

    const params: Record<string, unknown> = {};
    if (node.widgets) {
      for (const w of node.widgets) {
        if (!w.name) continue;
        params[w.name] = (w as { value: unknown }).value;
      }
    }
    nodes.push({
      id,
      type: node.type as string,
      params,
      pos: [Math.round(node.pos?.[0] ?? 0), Math.round(node.pos?.[1] ?? 0)],
    });
  }

  const links = graph.links;
  if (links) {
    const linkIter: Iterable<unknown> =
      typeof (links as { values?: () => Iterable<unknown> }).values === "function"
        ? (links as { values: () => Iterable<unknown> }).values()
        : (Object.values(links) as unknown as Iterable<unknown>);
    for (const raw of linkIter) {
      if (!raw) continue;
      const link = raw as {
        origin_id: number;
        origin_slot: number;
        target_id: number;
        target_slot: number;
      };
      const srcNode = findById(nodeList, link.origin_id);
      const dstNode = findById(nodeList, link.target_id);
      if (!srcNode || !dstNode) continue;
      const srcPort = srcNode.outputs?.[link.origin_slot]?.name;
      const dstPort = dstNode.inputs?.[link.target_slot]?.name;
      const srcId = idOf.get(link.origin_id);
      const dstId = idOf.get(link.target_id);
      if (!srcPort || !dstPort || !srcId || !dstId) continue;
      edges.push({
        src_node: srcId,
        src_port: srcPort,
        dst_node: dstId,
        dst_port: dstPort,
      });
    }
  }

  return { nodes, edges };
}

function shortId(type: string): string {
  const tail = type.split("/").pop() ?? type;
  return tail.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
}

function uniqueId(base: string, used: Set<string>): string {
  if (!used.has(base)) return base;
  let i = 2;
  while (used.has(`${base}_${i}`)) i++;
  return `${base}_${i}`;
}

function findById(nodes: LGraphNode[], id: number): LGraphNode | undefined {
  for (const n of nodes) if ((n.id as unknown as number) === id) return n;
  return undefined;
}
