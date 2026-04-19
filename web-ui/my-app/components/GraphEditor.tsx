"use client";

import {
  useCallback,
  useDeferredValue,
  useEffect,
  useRef,
  useState,
} from "react";

import "@comfyorg/litegraph/style.css";
import { LGraph, LGraphCanvas, LiteGraph } from "@comfyorg/litegraph";

import type { GraphDoc, NodeSchema, RunResult } from "@/lib/types";
import { loadIntoGraph, registerNodeTypes, saveFromGraph } from "@/lib/convert";

type Status =
  | { kind: "idle" | "ok" | "err" | "warn" | "loading"; text: string }
  | null;

type ThemeMode = "dark" | "light";

interface LGContextHandles {
  graph: import("@comfyorg/litegraph").LGraph;
  canvas: import("@comfyorg/litegraph").LGraphCanvas;
}

interface CanvasThemePalette {
  canvasBg: string;
  gridMinor: string;
  gridMajor: string;
  link: string;
  inputOff: string;
  inputOn: string;
  outputOff: string;
  outputOn: string;
  nodeTitle: string;
  nodeTitleSelected: string;
  nodeText: string;
  nodeSurface: string;
  nodeSurfaceLight: string;
  nodeOutline: string;
  widgetBg: string;
  nodeCategory: Record<NodeCategory, { color: string; bgcolor: string; boxcolor: string }>;
}

type NodeCategory = "data" | "forecast" | "risk" | "opt" | "general";

const SIDEBAR_WIDTH_KEY = "mapo.sidebar.width";
const THEME_KEY = "mapo.editor.theme";
const SIDEBAR_MIN = 220;
const SIDEBAR_MAX = 520;
const SIDEBAR_DEFAULT = 276;

const CANVAS_THEME: Record<ThemeMode, CanvasThemePalette> = {
  dark: {
    canvasBg: "#10161d",
    gridMinor: "rgba(145, 168, 189, 0.08)",
    gridMajor: "rgba(145, 168, 189, 0.18)",
    link: "#60c5d7",
    inputOff: "#7f5533",
    inputOn: "#ffb066",
    outputOff: "#36598c",
    outputOn: "#72a7ff",
    nodeTitle: "#dfe6ee",
    nodeTitleSelected: "#ffffff",
    nodeText: "#d7dee6",
    nodeSurface: "#27313b",
    nodeSurfaceLight: "#313d49",
    nodeOutline: "#5b6a7a",
    widgetBg: "#161d24",
    nodeCategory: {
      data: { color: "#314b6a", bgcolor: "#3e618b", boxcolor: "#75a8e4" },
      forecast: { color: "#35563a", bgcolor: "#45724d", boxcolor: "#88d497" },
      risk: { color: "#5a3f2f", bgcolor: "#7b553d", boxcolor: "#e6a26f" },
      opt: { color: "#47395f", bgcolor: "#5b4a7d", boxcolor: "#bca0ef" },
      general: { color: "#33414f", bgcolor: "#465869", boxcolor: "#94a8bc" },
    },
  },
  light: {
    canvasBg: "#f3f6f9",
    gridMinor: "rgba(59, 84, 109, 0.1)",
    gridMajor: "rgba(59, 84, 109, 0.18)",
    link: "#13879b",
    inputOff: "#b9783d",
    inputOn: "#e77819",
    outputOff: "#5c7fc5",
    outputOn: "#1f56c3",
    nodeTitle: "#2f4156",
    nodeTitleSelected: "#101826",
    nodeText: "#223141",
    nodeSurface: "#ffffff",
    nodeSurfaceLight: "#e8eef5",
    nodeOutline: "#9db0c5",
    widgetBg: "#dfe8f1",
    nodeCategory: {
      data: { color: "#8fb7f1", bgcolor: "#d7e6fb", boxcolor: "#4475bb" },
      forecast: { color: "#abddb0", bgcolor: "#dff3e2", boxcolor: "#4d8e58" },
      risk: { color: "#f0c39d", bgcolor: "#f9eadf", boxcolor: "#b06a37" },
      opt: { color: "#dbc6f8", bgcolor: "#efe7fd", boxcolor: "#7856af" },
      general: { color: "#c7d5e4", bgcolor: "#eef3f8", boxcolor: "#62778f" },
    },
  },
};

export default function GraphEditor() {
  const [schemas, setSchemas] = useState<NodeSchema[] | null>(null);
  const [status, setStatus] = useState<Status>(null);
  const [dirty, setDirty] = useState(false);
  const [theme, setTheme] = useState<ThemeMode>("dark");
  const [paletteQuery, setPaletteQuery] = useState("");
  const [sidebarWidth, setSidebarWidth] = useState(SIDEBAR_DEFAULT);
  const [hud, setHud] = useState({ nodes: 0, edges: 0 });
  const [running, setRunning] = useState(false);
  const [runResult, setRunResult] = useState<RunResult | null>(null);

  const deferredQuery = useDeferredValue(paletteQuery);
  const canvasElRef = useRef<HTMLCanvasElement | null>(null);
  const workspaceRef = useRef<HTMLElement | null>(null);
  const lgRef = useRef<LGContextHandles | null>(null);
  const schemasRef = useRef<NodeSchema[] | null>(null);
  const resizingRef = useRef(false);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const savedTheme = window.localStorage.getItem(THEME_KEY);
    if (savedTheme === "dark" || savedTheme === "light") {
      setTheme(savedTheme);
    } else if (window.matchMedia("(prefers-color-scheme: light)").matches) {
      setTheme("light");
    }

    const rawSidebarWidth = Number(window.localStorage.getItem(SIDEBAR_WIDTH_KEY));
    if (Number.isFinite(rawSidebarWidth)) {
      setSidebarWidth(clamp(rawSidebarWidth, SIDEBAR_MIN, SIDEBAR_MAX));
    }
  }, []);

  const refreshHud = useCallback((doc?: GraphDoc) => {
    if (doc) {
      setHud({ nodes: doc.nodes.length, edges: doc.edges.length });
      return;
    }
    const handles = lgRef.current;
    if (!handles) return;
    setHud({
      nodes: handles.graph._nodes?.length ?? 0,
      edges: countLinks(handles.graph),
    });
  }, []);

  const loadPersistedGraph = useCallback(async () => {
    const handles = lgRef.current;
    if (!handles) return;
    setStatus({ kind: "loading", text: "loading graph…" });
    try {
      const response = await fetch("/api/graph");
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || body.error || String(response.status));
      }
      const doc = (await response.json()) as GraphDoc;
      loadIntoGraph(handles.graph, doc);
      applyGraphTheme(handles.graph, theme);
      handles.graph.start();
      refreshHud(doc);
      setDirty(false);
      scheduleFit(lgRef);
      setStatus({
        kind: "ok",
        text: `loaded graph · ${doc.nodes.length} nodes, ${doc.edges.length} edges`,
      });
    } catch (error) {
      setStatus({ kind: "err", text: `graph load failed: ${errorMessage(error)}` });
    }
  }, [refreshHud]);

  const fetchSchemas = useCallback(async () => {
    setStatus({ kind: "loading", text: "loading schemas…" });
    try {
      const response = await fetch("/api/graph/schemas");
      if (!response.ok) throw new Error(String(response.status));
      const loaded = (await response.json()) as NodeSchema[];
      registerNodeTypes(loaded);
      schemasRef.current = loaded;
      setSchemas(loaded);
      await loadPersistedGraph();
    } catch (error) {
      setStatus({ kind: "err", text: `schema load failed: ${errorMessage(error)}` });
    }
  }, [loadPersistedGraph]);

  useEffect(() => {
    if (!canvasElRef.current || lgRef.current) return;
    try {
      const graph = new LGraph();
      const canvas = new LGraphCanvas(canvasElRef.current, graph);
      canvas.background_image = "";
      canvas.render_shadows = false;
      canvas.allow_searchbox = true;
      canvas.show_info = false;
      canvas.bgcanvas = canvas.canvas;
      canvas.bgctx = canvas.ctx;
      canvas.zoom_speed = 1.04;
      canvas.ds.min_scale = 0.2;
      canvas.ds.max_scale = 3;
      canvas.render_canvas_border = false;
      canvas.render_connections_border = false;
      canvas.connections_width = 3;
      graph.onAfterChange = () => {
        setDirty(true);
        refreshHud();
      };
      lgRef.current = { graph, canvas };
      applyCanvasTheme(canvas, theme);
      void fetchSchemas();
    } catch (error) {
      setStatus({ kind: "err", text: `boot failed: ${String(error)}` });
    }
  }, [fetchSchemas, refreshHud, theme]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(THEME_KEY, theme);
    const handles = lgRef.current;
    if (!handles) return;
    applyCanvasTheme(handles.canvas, theme);
    applyGraphTheme(handles.graph, theme);
    handles.canvas.setDirty(true, true);
  }, [theme]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(SIDEBAR_WIDTH_KEY, String(Math.round(sidebarWidth)));
  }, [sidebarWidth]);

  const persistCurrentGraph = useCallback(async () => {
    const handles = lgRef.current;
    if (!handles) throw new Error("editor not ready");
    const doc = saveFromGraph(handles.graph);
    const response = await fetch("/api/graph", {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(doc),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.detail || body.error || String(response.status));
    }
    setDirty(false);
    refreshHud(doc);
    return doc;
  }, [refreshHud]);

  const save = useCallback(async () => {
    setStatus({ kind: "loading", text: "saving graph…" });
    try {
      const doc = await persistCurrentGraph();
      setStatus({
        kind: "ok",
        text: `saved graph · ${doc.nodes.length} nodes, ${doc.edges.length} edges`,
      });
    } catch (error) {
      setStatus({ kind: "err", text: `save failed: ${errorMessage(error)}` });
    }
  }, [persistCurrentGraph]);

  const reload = useCallback(async () => {
    if (dirty) {
      const ok = window.confirm("Discard unsaved changes?");
      if (!ok) return;
    }
    await loadPersistedGraph();
  }, [dirty, loadPersistedGraph]);

  const run = useCallback(async () => {
    setRunning(true);
    setRunResult(null);
    setStatus({ kind: "loading", text: "saving and running…" });
    try {
      await persistCurrentGraph();
      const response = await fetch("/api/graph/run", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ticks: 1 }),
      });
      const body = (await response.json().catch(() => ({}))) as Partial<RunResult> & {
        error?: string;
        detail?: string;
      };
      if (!response.ok) {
        const failure: RunResult = {
          ok: false,
          code: Number(body.code ?? -1),
          stdout: String(body.stdout ?? ""),
          stderr: String(body.stderr ?? body.detail ?? body.error ?? ""),
        };
        setRunResult(failure);
        throw new Error(firstLine(failure.stderr || failure.stdout || "run failed"));
      }
      const result = body as RunResult;
      setRunResult(result);
      setStatus({
        kind: "ok",
        text: `run completed · ${hud.nodes} nodes validated and executed`,
      });
    } catch (error) {
      setStatus({ kind: "err", text: `run failed: ${errorMessage(error)}` });
    } finally {
      setRunning(false);
    }
  }, [hud.nodes, persistCurrentGraph]);

  const addNode = useCallback(
    (typeName: string) => {
      const handles = lgRef.current;
      if (!handles) return;
      const node = LiteGraph.createNode(typeName);
      if (!node) return;
      const offset = handles.graph._nodes?.length ?? 0;
      node.pos = [60 + (offset % 6) * 40, 60 + Math.floor(offset / 6) * 40];
      handles.graph.add(node);
      applyGraphTheme(handles.graph, theme);
      handles.graph.setDirtyCanvas(true, true);
      const doc = saveFromGraph(handles.graph);
      refreshHud(doc);
      setStatus({
        kind: "warn",
        text: `editing graph · ${doc.nodes.length} nodes, ${doc.edges.length} edges`,
      });
      setDirty(true);
    },
    [refreshHud, theme],
  );

  const toggleTheme = useCallback(() => {
    setTheme((current) => (current === "dark" ? "light" : "dark"));
  }, []);

  const startSidebarResize = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    resizingRef.current = true;
    event.currentTarget.setPointerCapture(event.pointerId);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  }, []);

  useEffect(() => {
    function stopResize() {
      if (!resizingRef.current) return;
      resizingRef.current = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    }

    function onPointerMove(event: PointerEvent) {
      if (!resizingRef.current) return;
      const workspace = workspaceRef.current;
      if (!workspace) return;
      const rect = workspace.getBoundingClientRect();
      const maxWidth = Math.min(SIDEBAR_MAX, Math.max(SIDEBAR_MIN, rect.width - 320));
      const next = clamp(event.clientX - rect.left, SIDEBAR_MIN, maxWidth);
      setSidebarWidth(next);
    }

    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", stopResize);
    window.addEventListener("pointercancel", stopResize);
    return () => {
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", stopResize);
      window.removeEventListener("pointercancel", stopResize);
      stopResize();
    };
  }, []);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") {
        event.preventDefault();
        void save();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [save]);

  useEffect(() => {
    const el = canvasElRef.current;
    if (!el) return;

    const onWheel = (event: WheelEvent) => {
      if (!event.ctrlKey) return;
      const handles = lgRef.current;
      if (!handles) return;
      event.preventDefault();
      event.stopImmediatePropagation();

      const { ds } = handles.canvas;
      const factor = Math.exp(-event.deltaY * 0.0012);
      const nextScale = clamp(ds.scale * factor, ds.min_scale ?? 0.2, ds.max_scale ?? 3);
      ds.changeScale(nextScale, [event.clientX, event.clientY], false);
      handles.graph.change();
      handles.canvas.setDirty(true, true);
    };

    el.addEventListener("wheel", onWheel, { passive: false, capture: true });
    return () => el.removeEventListener("wheel", onWheel, true);
  }, []);

  useEffect(() => {
    const el = canvasElRef.current;
    if (!el) return;
    function resize() {
      const handles = lgRef.current;
      if (!el || !handles) return;
      const parent = el.parentElement;
      if (!parent) return;
      const cssW = parent.clientWidth;
      const cssH = parent.clientHeight;
      if (cssW < 1 || cssH < 1) return;
      const dpr = window.devicePixelRatio || 1;
      handles.canvas.resize(
        Math.max(1, Math.floor(cssW * dpr)),
        Math.max(1, Math.floor(cssH * dpr)),
      );
      handles.canvas.setDirty(true, true);
    }
    resize();
    const ro = new ResizeObserver(resize);
    ro.observe(el.parentElement as Element);
    window.addEventListener("resize", resize);
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", resize);
    };
  }, [schemas]);

  const fitView = useCallback(() => {
    const handles = lgRef.current;
    if (!handles) return;
    fitCanvasToGraph(handles.canvas, handles.graph);
  }, []);

  const allItems = (schemas ?? []).toSorted((a, b) => a.type.localeCompare(b.type));
  const query = deferredQuery.trim().toLowerCase();
  const paletteItems = !query
    ? allItems
    : allItems.filter((schema) => {
        const text = `${schema.type} ${schema.category} ${schema.doc ?? ""}`.toLowerCase();
        return text.includes(query);
      });

  return (
    <div className={`editor-root theme-${theme}`}>
      <header className="editor-topbar">
        <div className="editor-brand">
          <span className="mark" aria-hidden="true" />
          <div className="editor-brand-copy">
            <span className="name">ML-MAPO</span>
            <span className="subhead">Unified Graph Editor</span>
          </div>
        </div>
        <div className="editor-spacer" />
        <div className="editor-actions">
          <span
            className={"editor-status" + (status ? " " + status.kind : "")}
            role="status"
            aria-live="polite"
            data-testid="editor-hud"
          >
            <span className="dot" aria-hidden="true" />
            <span className="text">{status?.text ?? "ready"}</span>
            {dirty ? <span className="dirty">unsaved</span> : null}
          </span>
          <button className="editor-btn" onClick={fitView} title="Fit graph to view">
            Fit
          </button>
          <button className="editor-btn" onClick={reload} title="Reload graph from disk">
            Reload
          </button>
          <button
            className="editor-btn"
            onClick={toggleTheme}
            title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
          >
            {theme === "dark" ? "Light Mode" : "Dark Mode"}
          </button>
          <button
            className="editor-btn"
            onClick={run}
            disabled={running}
            title="Save the current graph and execute one tick"
          >
            {running ? "Running…" : "Run"}
          </button>
          <button
            className="editor-btn primary"
            onClick={save}
            title="Save graph to disk (⌘/Ctrl+S)"
          >
            Save
          </button>
        </div>
      </header>

      <main
        ref={workspaceRef}
        className="editor-workspace"
        style={{ ["--sidebar-width" as string]: `${sidebarWidth}px` }}
      >
        <aside className="editor-palette" aria-label="Node palette">
          <header className="editor-panel-header">
            <h3>Nodes</h3>
            <span className="count">
              {paletteItems.length}
              {paletteItems.length !== allItems.length ? `/${allItems.length}` : ""}
            </span>
          </header>
          <div className="editor-palette-search">
            <input
              className="editor-search-input"
              type="search"
              value={paletteQuery}
              onChange={(event) => setPaletteQuery(event.target.value)}
              placeholder="Filter node types"
              aria-label="Filter node types"
            />
          </div>
          <div className="editor-palette-list">
            {paletteItems.map((schema) => {
              const label = schema.type.split("/").slice(1).join("/") || schema.type;
              return (
                <button
                  key={schema.type}
                  className="editor-palette-item"
                  data-cat={schema.category}
                  data-type={schema.type}
                  title={schema.doc || schema.type}
                  onClick={() => addNode(schema.type)}
                >
                  <span className="swatch" aria-hidden="true" />
                  <span className="type">{label}</span>
                  <span className="plus" aria-hidden="true">
                    +
                  </span>
                </button>
              );
            })}
            {paletteItems.length === 0 && schemas ? (
              <p className="editor-palette-empty">No node types match this filter.</p>
            ) : null}
          </div>
          <footer className="editor-palette-hint">
            <span>Click to add · drag ports to wire</span>
            <span>
              <kbd>⌘S</kbd> save
            </span>
          </footer>
        </aside>

        <div
          className="editor-resizer"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize node palette"
          onPointerDown={startSidebarResize}
          onDoubleClick={() => setSidebarWidth(SIDEBAR_DEFAULT)}
          title="Drag to resize the node palette"
        />

        <section className="editor-canvas-wrap">
          <canvas ref={canvasElRef} className="editor-canvas" tabIndex={0} />
          <div className="editor-canvas-meta" aria-hidden="true">
            unified graph · {hud.nodes} nodes · {hud.edges} edges
          </div>
          {runResult ? (
            <div className={`editor-console ${runResult.ok ? "ok" : "err"}`}>
              <div className="editor-console-title">
                {runResult.ok ? "Last Run" : "Run Error"}
              </div>
              <pre>{formatRunResult(runResult)}</pre>
            </div>
          ) : null}
        </section>
      </main>
    </div>
  );
}

function scheduleFit(
  ref: React.MutableRefObject<LGContextHandles | null>,
  attempt = 0,
): void {
  requestAnimationFrame(() => {
    const handles = ref.current;
    if (!handles) return;
    const el = handles.canvas.canvas as HTMLCanvasElement | null;
    const rect = el?.getBoundingClientRect();
    const ready = rect && rect.width > 32 && rect.height > 32;
    if (ready) {
      fitCanvasToGraph(handles.canvas, handles.graph);
      return;
    }
    if (attempt < 20) scheduleFit(ref, attempt + 1);
  });
}

function fitCanvasToGraph(
  canvas: import("@comfyorg/litegraph").LGraphCanvas,
  graph: import("@comfyorg/litegraph").LGraph,
): void {
  const nodes = graph._nodes ?? [];
  if (!nodes.length) return;

  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (const node of nodes) {
    const [x, y] = (node.pos as [number, number]) ?? [0, 0];
    const [w, h] = (node.size as [number, number]) ?? [120, 60];
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x + w > maxX) maxX = x + w;
    if (y + h > maxY) maxY = y + h;
  }
  if (!Number.isFinite(minX)) return;

  const pad = 60;
  const bounds: [number, number, number, number] = [
    minX - pad,
    minY - pad,
    maxX - minX + pad * 2,
    maxY - minY + pad * 2,
  ];
  canvas.ds.fitToBounds(bounds, { zoom: 0.9 });
  canvas.setDirty(true, true);
}

function countLinks(graph: import("@comfyorg/litegraph").LGraph): number {
  const links = graph.links;
  if (!links) return 0;
  if (typeof (links as { size?: number }).size === "number") {
    return (links as { size: number }).size;
  }
  return Object.keys(links as unknown as Record<string, unknown>).length;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function firstLine(text: string): string {
  return text.split(/\r?\n/).find((line) => line.trim())?.trim() ?? text;
}

function formatRunResult(result: RunResult): string {
  const output = result.stderr || result.stdout || "No output.";
  const trimmed = output.trim();
  return trimmed.length > 2400 ? `${trimmed.slice(0, 2400)}\n…` : trimmed;
}

function applyGraphTheme(
  graph: import("@comfyorg/litegraph").LGraph,
  theme: ThemeMode,
): void {
  const palette = CANVAS_THEME[theme];
  const nodes = graph._nodes ?? [];
  for (const rawNode of nodes) {
    const node = rawNode as import("@comfyorg/litegraph").LGraphNode & {
      color?: string;
      bgcolor?: string;
      boxcolor?: string;
    };
    const category = categoryFromType(node.type as string);
    const colors = palette.nodeCategory[category];
    node.color = colors.color;
    node.bgcolor = colors.bgcolor;
    node.boxcolor = colors.boxcolor;
  }

  const links = graph.links;
  if (!links) return;
  const values =
    typeof (links as { values?: () => Iterable<unknown> }).values === "function"
      ? (links as { values: () => Iterable<unknown> }).values()
      : (Object.values(links as unknown as Record<string, unknown>) as Iterable<unknown>);
  for (const rawLink of values) {
    if (!rawLink || typeof rawLink !== "object") continue;
    (rawLink as { color?: string }).color = palette.link;
  }
}

function categoryFromType(type: string): NodeCategory {
  const prefix = type.split("/")[0];
  switch (prefix) {
    case "data":
      return "data";
    case "forecast":
      return "forecast";
    case "risk":
      return "risk";
    case "opt":
    case "optimization":
      return "opt";
    default:
      return "general";
  }
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

function applyCanvasTheme(
  canvas: import("@comfyorg/litegraph").LGraphCanvas,
  theme: ThemeMode,
): void {
  const palette = CANVAS_THEME[theme];
  canvas.clear_background_color = palette.canvasBg;
  canvas.default_link_color = palette.link;
  canvas.default_connection_color = {
    input_off: palette.inputOff,
    input_on: palette.inputOn,
    output_off: palette.outputOff,
    output_on: palette.outputOn,
  };
  canvas.default_connection_color_byType = {
    ohlcv_snapshot: palette.link,
    covariance: palette.link,
    alpha_series: palette.link,
    alpha_scores: palette.link,
    weights: palette.link,
    dict: palette.link,
    date: palette.link,
    Engine: palette.link,
    int: palette.link,
    str: palette.link,
  };
  canvas.default_connection_color_byTypeOff = {
    ohlcv_snapshot: palette.link,
    covariance: palette.link,
    alpha_series: palette.link,
    alpha_scores: palette.link,
    weights: palette.link,
    dict: palette.link,
    date: palette.link,
    Engine: palette.link,
    int: palette.link,
    str: palette.link,
  };
  LGraphCanvas.link_type_colors = {
    ...LGraphCanvas.link_type_colors,
    ohlcv_snapshot: palette.link,
    covariance: palette.link,
    alpha_series: palette.link,
    alpha_scores: palette.link,
    weights: palette.link,
    dict: palette.link,
    date: palette.link,
    Engine: palette.link,
    int: palette.link,
    str: palette.link,
  };
  canvas.onDrawBackground = createBackgroundRenderer(canvas, palette);

  LiteGraph.NODE_TITLE_COLOR = palette.nodeTitle;
  LiteGraph.NODE_SELECTED_TITLE_COLOR = palette.nodeTitleSelected;
  LiteGraph.NODE_TEXT_COLOR = palette.nodeText;
  LiteGraph.NODE_DEFAULT_COLOR = palette.nodeSurfaceLight;
  LiteGraph.NODE_DEFAULT_BGCOLOR = palette.nodeSurface;
  LiteGraph.NODE_DEFAULT_BOXCOLOR = palette.nodeOutline;
  LiteGraph.NODE_BOX_OUTLINE_COLOR = palette.nodeOutline;
  LiteGraph.WIDGET_BGCOLOR = palette.widgetBg;
}

function createBackgroundRenderer(
  canvas: import("@comfyorg/litegraph").LGraphCanvas,
  palette: CanvasThemePalette,
): (
  ctx: CanvasRenderingContext2D,
  visibleArea: [number, number, number, number],
) => void {
  return (ctx, visibleArea) => {
    const [x, y, width, height] = visibleArea;
    const scale = canvas.ds.scale || 1;
    const minor = 32;
    const major = minor * 5;

    ctx.save();
    ctx.lineWidth = 1 / scale;

    ctx.beginPath();
    ctx.strokeStyle = palette.gridMinor;
    for (let px = Math.floor(x / minor) * minor; px < x + width; px += minor) {
      ctx.moveTo(px, y);
      ctx.lineTo(px, y + height);
    }
    for (let py = Math.floor(y / minor) * minor; py < y + height; py += minor) {
      ctx.moveTo(x, py);
      ctx.lineTo(x + width, py);
    }
    ctx.stroke();

    ctx.beginPath();
    ctx.strokeStyle = palette.gridMajor;
    for (let px = Math.floor(x / major) * major; px < x + width; px += major) {
      ctx.moveTo(px, y);
      ctx.lineTo(px, y + height);
    }
    for (let py = Math.floor(y / major) * major; py < y + height; py += major) {
      ctx.moveTo(x, py);
      ctx.lineTo(x + width, py);
    }
    ctx.stroke();

    ctx.restore();
  };
}
