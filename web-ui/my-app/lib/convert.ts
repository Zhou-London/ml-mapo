import {
  LGraphCanvas,
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
export const SCHEMA_KEY = "__mapo_schema";
// Preserve the original node id from the doc across save/load round-trips.
export const NODE_ID_KEY = "__mapo_id";
// Per-run state attached by GraphEditor; read by onDrawForeground.
export const NODE_ACTIVE_KEY = "__mapo_active";
export const NODE_MS_KEY = "__mapo_ms";
// User flag — a "commented out" node. Persisted in graph.json; the Python
// loader drops disabled nodes (and their edges) before validating/executing.
export const NODE_DISABLED_KEY = "__mapo_disabled";

type MapoLGraphNode = LGraphNode & {
  [SCHEMA_KEY]?: NodeSchema;
  [NODE_ID_KEY]?: string;
  [NODE_ACTIVE_KEY]?: boolean;
  [NODE_MS_KEY]?: number;
  [NODE_DISABLED_KEY]?: boolean;
};

export function findMapoNode(graph: LGraph, mapoId: string): LGraphNode | undefined {
  const nodes = (graph._nodes ?? []) as MapoLGraphNode[];
  return nodes.find((node) => node[NODE_ID_KEY] === mapoId);
}

export function clearRunStatus(graph: LGraph): void {
  for (const raw of graph._nodes ?? []) {
    const node = raw as MapoLGraphNode;
    node[NODE_ACTIVE_KEY] = false;
    delete node[NODE_MS_KEY];
  }
}

// One distinct color per wire type. The same color is used for the wire and
// the input/output dots on either end (LiteGraph reads it via
// `link_type_colors` and `default_connection_color_byType`).
export const WIRE_COLORS: Record<string, string> = {
  date: "#ffa860",
  Engine: "#a0aabe",
  frame: "#5fc7b8",
  cov: "#ed6f9a",
  alpha: "#7ad17a",
  weights: "#b18ef0",
};

// Module-level theme so MapoNode draw callbacks can pick colors that read
// well on the current canvas background. Editor calls setEditorTheme() on
// every theme change.
type EditorTheme = "dark" | "light";
let currentTheme: EditorTheme = "dark";
export function setEditorTheme(theme: EditorTheme): void {
  currentTheme = theme;
}

// UI-only hints: example value shown in the field as a placeholder, and an
// optional regex used to flag obviously-wrong input. Keyed by param name so
// it works across every node type that exposes the same param.
interface ParamHint {
  example: string;
  pattern?: RegExp;
}
const PARAM_HINTS: Record<string, ParamHint> = {
  start_date: { example: "2024-01-15", pattern: /^(\d{4}-\d{2}-\d{2})?$/ },
  end_date: { example: "2024-12-31", pattern: /^(\d{4}-\d{2}-\d{2})?$/ },
  url: { example: "postgresql+psycopg2://user:pass@host:5432/db", pattern: /^[a-z][a-z0-9+.\-]*:\/\/.+/i },
  tickers: {
    example: "NVDA,AAPL,MSFT",
    pattern: /^[A-Z][A-Z0-9.\-=]*(,[A-Z][A-Z0-9.\-=]*)*$/,
  },
  factor: { example: "naive_sample_cov" },
  factors: { example: "momentum_12_1" },
  information_ratios: { example: "1.0", pattern: /^[\d.,\s-]*$/ },
  top: { example: "10", pattern: /^\d+$/ },
  risk_aversion: { example: "50", pattern: /^\d+(\.\d+)?$/ },
};

function paramStatus(name: string, value: string): "ok" | "empty" | "wrong" {
  const hint = PARAM_HINTS[name];
  if (!hint) return "ok";
  if (value.length === 0) return "empty";
  if (hint.pattern && !hint.pattern.test(value)) return "wrong";
  return "ok";
}

export interface ParamValidationError {
  node: string;
  param: string;
  value: string;
  example: string;
}

export function validateGraphParams(graph: LGraph): ParamValidationError[] {
  const errors: ParamValidationError[] = [];
  for (const raw of graph._nodes ?? []) {
    const node = raw as MapoLGraphNode;
    const id = node[NODE_ID_KEY] ?? String(node.id);
    for (const w of node.widgets ?? []) {
      const value = String((w as { value: unknown }).value ?? "");
      if (paramStatus(w.name ?? "", value) === "wrong") {
        errors.push({
          node: id,
          param: w.name ?? "",
          value,
          example: PARAM_HINTS[w.name ?? ""].example,
        });
      }
    }
  }
  return errors;
}

export function registerNodeTypes(schemas: NodeSchema[]): void {
  patchDrawNodeWidgets();
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

// LiteGraph draws widgets *after* onDrawForeground, so any per-widget overlay
// must happen after drawNodeWidgets. Patch once to call an extra hook on each
// node — guarded so HMR/multiple registerNodeTypes() calls don't re-wrap.
const PATCHED_KEY = "__mapo_widgets_patched";
function patchDrawNodeWidgets(): void {
  type Proto = typeof LGraphCanvas.prototype & {
    [PATCHED_KEY]?: boolean;
    drawNodeWidgets: (...args: unknown[]) => unknown;
  };
  const proto = LGraphCanvas.prototype as Proto;
  if (proto[PATCHED_KEY]) return;
  const original = proto.drawNodeWidgets;
  proto.drawNodeWidgets = function patched(...args: unknown[]) {
    const ret = original.apply(this, args);
    const node = args[0] as { onDrawAfterWidgets?: (ctx: CanvasRenderingContext2D) => void };
    node.onDrawAfterWidgets?.(args[2] as CanvasRenderingContext2D);
    return ret;
  };
  proto[PATCHED_KEY] = true;
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

    onDrawForeground(ctx: CanvasRenderingContext2D): void {
      const self = this as MapoLGraphNode;
      const [w, h] = (this.size ?? [0, 0]) as [number, number];
      const titleH = LiteGraph.NODE_TITLE_HEIGHT ?? 30;

      if (self[NODE_DISABLED_KEY]) {
        ctx.save();
        ctx.fillStyle = "rgba(20, 25, 32, 0.55)";
        ctx.fillRect(-1, -titleH - 1, w + 2, h + titleH + 2);
        ctx.fillStyle = "#ffd34d";
        ctx.font = "bold 11px ui-sans-serif, system-ui, sans-serif";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText("// commented out", w / 2, h / 2);
        ctx.restore();
        return; // skip the rest — no point drawing active border or ms
      }

      if (self[NODE_ACTIVE_KEY]) {
        ctx.save();
        ctx.strokeStyle = "#ffd34d";
        ctx.lineWidth = 3;
        ctx.shadowColor = "#ffd34d";
        ctx.shadowBlur = 12;
        ctx.strokeRect(-2, -titleH - 2, w + 4, h + titleH + 4);
        ctx.restore();
      }

      const ms = self[NODE_MS_KEY];
      if (typeof ms === "number") {
        ctx.save();
        ctx.font = "11px ui-monospace, SFMono-Regular, monospace";
        const msColor = currentTheme === "light" ? "#5c6b7a" : "#a5b4c0";
        ctx.fillStyle = self[NODE_ACTIVE_KEY] ? "#c08e00" : msColor;
        ctx.textAlign = "right";
        ctx.textBaseline = "bottom";
        ctx.fillText(formatMs(ms), w - 8, h - 6);
        ctx.restore();
      }
    }

    onDrawAfterWidgets(ctx: CanvasRenderingContext2D): void {
      const w = (this.size?.[0] ?? 0) as number;
      const widgetH = LiteGraph.NODE_WIDGET_HEIGHT ?? 20;
      const emptyColor = currentTheme === "light" ? "#5c6b7a" : "#9aa8b6";
      const wrongColor = currentTheme === "light" ? "#c7544d" : "#ff7a7a";
      for (const widget of this.widgets ?? []) {
        const hint = PARAM_HINTS[widget.name ?? ""];
        if (!hint) continue;
        const y = (widget as { last_y?: number }).last_y;
        if (typeof y !== "number") continue;
        const value = String((widget as { value: unknown }).value ?? "");
        const status = paramStatus(widget.name ?? "", value);
        if (status === "ok") continue;

        ctx.save();
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        if (status === "empty") {
          ctx.font = "italic 11px ui-sans-serif, system-ui, sans-serif";
          ctx.fillStyle = emptyColor;
          ctx.fillText(`e.g. ${hint.example}`, w - 14, y + widgetH / 2);
        } else {
          ctx.font = "bold 13px ui-sans-serif, system-ui, sans-serif";
          ctx.fillStyle = wrongColor;
          ctx.fillText("✗", w - 10, y + widgetH / 2);
        }
        ctx.restore();
      }
    }
  }
  Object.defineProperty(MapoNode, "name", { value: `MapoNode_${schema.type}` });
  return MapoNode;
}

function formatMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms >= 10) return `${ms.toFixed(1)} ms`;
  return `${ms.toFixed(2)} ms`;
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
    if (n.size) node.size = [n.size[0], n.size[1]];
    node[NODE_ID_KEY] = n.id;
    if (n.disabled) node[NODE_DISABLED_KEY] = true;

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
      size: [Math.round(node.size?.[0] ?? 0), Math.round(node.size?.[1] ?? 0)],
      ...(node[NODE_DISABLED_KEY] ? { disabled: true } : {}),
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
