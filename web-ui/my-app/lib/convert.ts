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
  ObserverSnapshot,
} from "./types";

// Stash the original schema on the node so saveFromGraph can recover the
// canonical input/output port names (LiteGraph may mutate its own slot list).
const SCHEMA_KEY = "__mapo_schema";
// Preserve the original node id from the doc across save/load round-trips.
const NODE_ID_KEY = "__mapo_id";
// Latest run snapshots for an Observer node, rendered in-place.
const OBSERVER_KEY = "__mapo_observer";

const OBSERVER_PADDING_X = 14;
const OBSERVER_PADDING_TOP = 10;
const OBSERVER_PADDING_BOTTOM = 12;
const OBSERVER_LINE_HEIGHT = 14;
const OBSERVER_INDENT_PX = 12;
const OBSERVER_FONT = "11px ui-monospace, 'SF Mono', Menlo, monospace";
const OBSERVER_HEADER_FONT =
  "600 11px ui-monospace, 'SF Mono', Menlo, monospace";
const OBSERVER_KEY_FONT =
  "600 11px ui-monospace, 'SF Mono', Menlo, monospace";
const OBSERVER_PLACEHOLDER = "no run yet · click Run to populate";
const OBSERVER_MIN_WIDTH = 280;
const OBSERVER_MAX_WIDTH = 460;
// LiteGraph slot and widget heights (constants on the LiteGraph singleton —
// duplicated here to keep this file decoupled from import-time globals).
const SLOT_HEIGHT = 20;
const WIDGET_HEIGHT = 20;

interface RenderedLine {
  text: string;
  indent: number;
  bold: boolean;
}

// Maximum number of fields in a one-line "key: f1=v1 f2=v2 …" inline
// summary for a record. Beyond this we trim with an ellipsis.
const INLINE_FIELD_CAP = 5;

type MapoLGraphNode = LGraphNode & {
  [SCHEMA_KEY]?: NodeSchema;
  [NODE_ID_KEY]?: string;
  [OBSERVER_KEY]?: ObserverSnapshot[];
};

export function isObserverType(type: string): boolean {
  return type.endsWith("/Observer");
}

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
  const observer = isObserverType(schema.type);

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
      if (observer) installObserverDrawing(this as MapoLGraphNode);
    }
  }
  Object.defineProperty(MapoNode, "name", { value: `MapoNode_${schema.type}` });
  return MapoNode;
}

/**
 * Apply the latest run report to the graph: each entry matched by node id
 * gets its snapshots stored and re-renders in-place. Returns the set of node
 * ids that received data so the caller can highlight them.
 */
export function applyObserverReport(
  graph: LGraph,
  observers: Record<string, ObserverSnapshot[]>,
): Set<string> {
  const updated = new Set<string>();
  const lookup = new Map<string, MapoLGraphNode>();
  for (const node of (graph._nodes ?? []) as MapoLGraphNode[]) {
    const id = node[NODE_ID_KEY];
    if (id) lookup.set(id, node);
  }
  for (const [nodeId, snapshots] of Object.entries(observers)) {
    if (!nodeId) continue;
    const node = lookup.get(nodeId);
    if (!node) continue;
    node[OBSERVER_KEY] = snapshots;
    resizeObserverNode(node);
    updated.add(nodeId);
  }
  return updated;
}

/**
 * Wipe stored Observer data — used when the graph is reloaded from disk so
 * stale numbers from a previous session don't linger on screen.
 */
export function clearObserverState(graph: LGraph): void {
  for (const node of (graph._nodes ?? []) as MapoLGraphNode[]) {
    if (node[OBSERVER_KEY]) {
      delete node[OBSERVER_KEY];
      resizeObserverNode(node);
    }
  }
}

function installObserverDrawing(node: MapoLGraphNode): void {
  // Layout: start with LiteGraph's own size for slots+widgets, then extend
  // the body downward to make room for the snapshot text region.
  const baseSize = node.computeSize?.() ?? [OBSERVER_MIN_WIDTH, 60];
  node.size = [
    Math.max(baseSize[0], OBSERVER_MIN_WIDTH),
    baseSize[1] + OBSERVER_PADDING_TOP + OBSERVER_PADDING_BOTTOM + OBSERVER_LINE_HEIGHT,
  ];

  // Litegraph calls onDrawForeground in node-local coords, with (0,0) at the
  // top-left of the node *body* (below the title bar). Slots and widgets are
  // drawn into that body too, so the snapshot text has to start *after*
  // them — otherwise it overlaps the port labels.
  node.onDrawForeground = function (this: MapoLGraphNode, ctx: CanvasRenderingContext2D) {
    const snapshots = this[OBSERVER_KEY] ?? [];
    const top = bodyContentTop(this);
    const left = OBSERVER_PADDING_X;
    const usableWidth = (this.size?.[0] ?? OBSERVER_MIN_WIDTH) - OBSERVER_PADDING_X * 2;

    ctx.save();
    ctx.textAlign = "left";
    ctx.textBaseline = "top";

    if (!snapshots.length) {
      ctx.fillStyle = "rgba(140, 152, 168, 0.78)";
      ctx.font = OBSERVER_FONT;
      ctx.fillText(OBSERVER_PLACEHOLDER, left, top);
      ctx.restore();
      return;
    }

    let y = top;
    for (const snapshot of snapshots) {
      ctx.fillStyle = "rgba(255, 255, 255, 0.96)";
      ctx.font = OBSERVER_HEADER_FONT;
      ctx.fillText(`▸ ${snapshot.name}`, left, y);
      y += OBSERVER_LINE_HEIGHT;

      for (const line of renderSnapshotLines(snapshot.data)) {
        ctx.font = line.bold ? OBSERVER_KEY_FONT : OBSERVER_FONT;
        ctx.fillStyle = line.bold
          ? "rgba(255, 255, 255, 0.92)"
          : "rgba(228, 234, 244, 0.86)";
        const x = left + line.indent * OBSERVER_INDENT_PX;
        const text = clipText(ctx, line.text, usableWidth - line.indent * OBSERVER_INDENT_PX);
        ctx.fillText(text, x, y);
        y += OBSERVER_LINE_HEIGHT;
      }
      y += 4;
    }
    ctx.restore();
  };
}

function resizeObserverNode(node: MapoLGraphNode): void {
  const top = bodyContentTop(node);
  const snapshots = node[OBSERVER_KEY] ?? [];
  const totalLines = snapshots.length
    ? snapshots.reduce((sum, s) => sum + 1 + renderSnapshotLines(s.data).length, 0)
    : 1;
  const textBlockHeight =
    totalLines * OBSERVER_LINE_HEIGHT + (snapshots.length ? snapshots.length * 4 : 0);
  const desiredWidth = clamp(
    node.size?.[0] ?? OBSERVER_MIN_WIDTH,
    OBSERVER_MIN_WIDTH,
    OBSERVER_MAX_WIDTH,
  );
  // Total body = slots+widgets region (top) + the snapshot text block.
  node.size = [desiredWidth, top + textBlockHeight + OBSERVER_PADDING_BOTTOM];
  node.setDirtyCanvas?.(true, true);
}

/**
 * Y offset (in node-local body coords) where the snapshot text region starts.
 *
 * LiteGraph stacks: input/output slots at the top of the body, then widgets
 * below them. We need to clear both before drawing so port labels don't get
 * overwritten. Heights match LiteGraph's NODE_SLOT_HEIGHT / WIDGET_HEIGHT
 * defaults (20px each).
 */
function bodyContentTop(node: LGraphNode): number {
  const inputCount = node.inputs?.length ?? 0;
  const outputCount = node.outputs?.length ?? 0;
  const slotRows = Math.max(inputCount, outputCount);
  const widgetCount = node.widgets?.length ?? 0;
  return slotRows * SLOT_HEIGHT + widgetCount * WIDGET_HEIGHT + OBSERVER_PADDING_TOP;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

/**
 * Flatten a snapshot payload into a list of indented lines that read like a
 * key/value map. The full structure is preserved (no truncation) so a node
 * grows as tall as the data demands — the user wanted to see *everything*,
 * not a summary.
 *
 * Rules:
 *   - Scalars render as `key: value` on one line.
 *   - Nested dicts get a `key:` header with their fields indented.
 *   - Arrays of homogeneous {label, value}-shaped dicts (e.g. top_holdings:
 *     [{ticker, weight}]) render as `key:` + indented `label: value` rows.
 *   - Other arrays render their items one per line (or inline for short
 *     scalar arrays).
 */
function renderSnapshotLines(data: Record<string, unknown>, indent = 0): RenderedLine[] {
  const lines: RenderedLine[] = [];
  for (const [key, value] of Object.entries(data)) {
    if (key === "seq") continue;
    appendValue(lines, key, value, indent);
  }
  return lines;
}

function appendValue(
  lines: RenderedLine[],
  key: string,
  value: unknown,
  indent: number,
): void {
  if (value == null) {
    lines.push({ text: `${key}: null`, indent, bold: true });
    return;
  }
  if (Array.isArray(value)) {
    appendArray(lines, key, value, indent);
    return;
  }
  if (typeof value === "object") {
    const dict = value as Record<string, unknown>;
    const entries = Object.entries(dict);
    if (entries.length === 0) {
      lines.push({ text: `${key}: {}`, indent, bold: true });
      return;
    }
    lines.push({ text: `${key}:`, indent, bold: true });
    // Detect "table" shape — every value is itself a homogeneous record. In
    // that case render one inline line per entry so 55-asset trace dicts
    // don't blow up into 600-line walls.
    if (isDictOfRecords(dict)) {
      for (const [k, v] of entries) {
        lines.push({
          text: `${k}: ${inlineRecord(v as Record<string, unknown>)}`,
          indent: indent + 1,
          bold: false,
        });
      }
      return;
    }
    for (const [k, v] of entries) appendValue(lines, k, v, indent + 1);
    return;
  }
  lines.push({ text: `${key}: ${formatScalar(value)}`, indent, bold: false });
}

function isDictOfRecords(dict: Record<string, unknown>): boolean {
  const values = Object.values(dict);
  if (values.length < 2) return false;
  for (const v of values) {
    if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  }
  return true;
}

function inlineRecord(record: Record<string, unknown>): string {
  const entries = Object.entries(record);
  const visible = entries.slice(0, INLINE_FIELD_CAP);
  const parts = visible.map(([k, v]) => `${k}=${formatScalarShort(v)}`);
  if (entries.length > visible.length) parts.push(`+${entries.length - visible.length}`);
  return parts.join("  ");
}

function appendArray(
  lines: RenderedLine[],
  key: string,
  arr: unknown[],
  indent: number,
): void {
  if (arr.length === 0) {
    lines.push({ text: `${key}: []`, indent, bold: true });
    return;
  }

  // Array of label/value dicts (e.g. [{ticker:'AAPL', weight:0.05}]) →
  // render as a clean two-column-ish key/value list keyed by the natural
  // label field.
  const labelValue = labelValueShape(arr);
  if (labelValue) {
    lines.push({ text: `${key}:`, indent, bold: true });
    for (const row of arr as Record<string, unknown>[]) {
      const label = formatScalar(row[labelValue.labelKey]);
      const val = formatScalar(row[labelValue.valueKey]);
      lines.push({ text: `${label}: ${val}`, indent: indent + 1, bold: false });
    }
    return;
  }

  // Array of larger dicts → one inline line per item using the first scalar
  // field as the row label, so 55-asset traces stay legible.
  if (arr.every((it) => it && typeof it === "object" && !Array.isArray(it))) {
    lines.push({ text: `${key}:`, indent, bold: true });
    for (const item of arr as Record<string, unknown>[]) {
      const entries = Object.entries(item);
      const labelKey = entries.find(([, v]) => typeof v !== "object")?.[0];
      if (labelKey) {
        const others = entries.filter(([k]) => k !== labelKey);
        const summary = inlineRecord(Object.fromEntries(others));
        lines.push({
          text: `${formatScalar(item[labelKey])}: ${summary}`,
          indent: indent + 1,
          bold: false,
        });
      } else {
        // No obvious label — fall back to a generic indented record block.
        lines.push({ text: `-`, indent: indent + 1, bold: true });
        for (const [k, v] of entries) appendValue(lines, k, v, indent + 2);
      }
    }
    return;
  }

  // Array of scalars → inline if it'll fit on one line, otherwise one per row.
  const scalars = arr.map((v) => formatScalar(v));
  const inline = scalars.join(", ");
  if (inline.length <= 64) {
    lines.push({ text: `${key}: [${inline}]`, indent, bold: false });
    return;
  }
  lines.push({ text: `${key}:`, indent, bold: true });
  for (const v of scalars) {
    lines.push({ text: v, indent: indent + 1, bold: false });
  }
}

/**
 * Detect whether an array looks like [{label_field, value_field}, …] — a
 * common shape in the snapshot payloads (top_holdings, top_vol, factor lists).
 * Returns the resolved label/value key names if so, otherwise null.
 */
function labelValueShape(
  arr: unknown[],
): { labelKey: string; valueKey: string } | null {
  if (arr.length === 0) return null;
  const first = arr[0];
  if (!first || typeof first !== "object" || Array.isArray(first)) return null;
  const keys = Object.keys(first as Record<string, unknown>);
  if (keys.length !== 2) return null;
  // Make sure every entry has the same two keys with the same value-type
  // pattern (one scalar label, one scalar value).
  for (const item of arr) {
    if (!item || typeof item !== "object") return null;
    const entry = item as Record<string, unknown>;
    if (Object.keys(entry).length !== 2) return null;
    for (const k of keys) {
      const v = entry[k];
      if (v != null && typeof v === "object") return null;
    }
  }
  // Heuristic: prefer string-typed key as label, numeric as value. If both
  // strings or both numbers, fall back to the declaration order.
  const [a, b] = keys;
  const va = (first as Record<string, unknown>)[a];
  const vb = (first as Record<string, unknown>)[b];
  if (typeof va === "number" && typeof vb !== "number") {
    return { labelKey: b, valueKey: a };
  }
  return { labelKey: a, valueKey: b };
}

function formatScalarShort(value: unknown): string {
  if (value == null) return "null";
  if (typeof value === "object") return Array.isArray(value) ? `[${value.length}]` : "{…}";
  return formatScalar(value);
}

function formatScalar(value: unknown): string {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return String(value);
    if (Number.isInteger(value) && Math.abs(value) < 1e6) return String(value);
    return Math.abs(value) >= 1000 || Math.abs(value) < 0.001
      ? value.toExponential(3)
      : value.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
  }
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

function clipText(ctx: CanvasRenderingContext2D, text: string, maxWidth: number): string {
  if (ctx.measureText(text).width <= maxWidth) return text;
  let lo = 0;
  let hi = text.length;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (ctx.measureText(text.slice(0, mid) + "…").width <= maxWidth) lo = mid;
    else hi = mid - 1;
  }
  return text.slice(0, lo) + "…";
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
    if (isObserverType(n.type)) resizeObserverNode(node);
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
