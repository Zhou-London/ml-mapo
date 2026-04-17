"use client";

/**
 * LiteGraph-backed editor. Client component because LiteGraph imports the DOM
 * at module eval time, which would crash in any RSC / SSR pass.
 *
 * Contract:
 *   - loads schemas once via /api/graph/schemas and registers them with
 *     LiteGraph.
 *   - per active module, loads /api/graph/<mod> and hydrates the canvas via
 *     loadIntoGraph(); save path runs saveFromGraph() and PUTs the result.
 *   - the palette is just the subset of schemas whose type prefix matches
 *     the selected module ("opt/" for optimization, otherwise "<mod>/").
 */

import { useCallback, useEffect, useRef, useState } from "react";

// LiteGraph ships its own canvas stylesheet. Statically import here so
// Next.js can fold it into the page bundle instead of relying on a runtime
// `await import()` of a CSS file (which browsers don't support).
import "@comfyorg/litegraph/style.css";
import { LGraph, LGraphCanvas, LiteGraph } from "@comfyorg/litegraph";

import {
  MODULES,
  MODULE_PREFIX,
  type GraphDoc,
  type ModuleName,
  type NodeSchema,
} from "@/lib/types";
import {
  loadIntoGraph,
  registerNodeTypes,
  saveFromGraph,
} from "@/lib/convert";

type Status =
  | { kind: "idle" | "ok" | "err" | "warn" | "loading"; text: string }
  | null;

interface LGContextHandles {
  graph: import("@comfyorg/litegraph").LGraph;
  canvas: import("@comfyorg/litegraph").LGraphCanvas;
}

export default function GraphEditor() {
  const [module, setModule] = useState<ModuleName>("data");
  const [schemas, setSchemas] = useState<NodeSchema[] | null>(null);
  const [status, setStatus] = useState<Status>(null);
  const [dirty, setDirty] = useState(false);
  const [hud, setHud] = useState<{ nodes: number; edges: number }>({
    nodes: 0,
    edges: 0,
  });

  const canvasElRef = useRef<HTMLCanvasElement | null>(null);
  const lgRef = useRef<LGContextHandles | null>(null);
  const schemasRef = useRef<NodeSchema[] | null>(null);

  /* ---------- boot LiteGraph once ---------- */
  useEffect(() => {
    if (!canvasElRef.current) return;
    if (lgRef.current) return; // strict-mode re-mount guard
    try {
      const graph = new LGraph();
      const canvas = new LGraphCanvas(canvasElRef.current, graph);
      canvas.background_image = "";
      canvas.render_shadows = false;
      canvas.allow_searchbox = true;
      graph.onAfterChange = () => setDirty(true);
      lgRef.current = { graph, canvas };
      fetchSchemas();
    } catch (e) {
      console.error("[editor] boot failed:", e);
      setStatus({ kind: "err", text: `boot failed: ${String(e)}` });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* ---------- schema fetch + register ---------- */
  const fetchSchemas = useCallback(async () => {
    setStatus({ kind: "loading", text: "loading schemas…" });
    try {
      const r = await fetch("/api/graph/schemas");
      if (!r.ok) throw new Error(`${r.status}`);
      const s = (await r.json()) as NodeSchema[];
      registerNodeTypes(s);
      schemasRef.current = s;
      setSchemas(s);
      setStatus({ kind: "ok", text: `schemas loaded (${s.length})` });
      // If a module is already selected, populate the canvas now that node
      // types exist. Otherwise switchModule will do it.
      await loadModule(module);
    } catch (e) {
      setStatus({ kind: "err", text: `schema load failed: ${String(e)}` });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* ---------- module load ---------- */
  const loadModule = useCallback(async (m: ModuleName) => {
    const handles = lgRef.current;
    if (!handles) return;
    setStatus({ kind: "loading", text: `loading ${m}…` });
    try {
      const r = await fetch(`/api/graph/${m}`);
      if (!r.ok) throw new Error(`${r.status}`);
      const doc = (await r.json()) as GraphDoc;
      loadIntoGraph(handles.graph, doc);
      handles.graph.start();
      refreshHud();
      setDirty(false);
      setStatus({
        kind: "ok",
        text: `loaded ${m} · ${doc.nodes.length} nodes, ${doc.edges.length} edges`,
      });
    } catch (e) {
      setStatus({ kind: "err", text: `load ${m} failed: ${String(e)}` });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* ---------- module switch ---------- */
  const onSwitchModule = useCallback(
    async (m: ModuleName) => {
      if (m === module) return;
      if (dirty) {
        const ok = window.confirm(
          `You have unsaved changes in ${module}. Discard them?`,
        );
        if (!ok) return;
      }
      setModule(m);
      if (schemasRef.current) await loadModule(m);
    },
    [module, dirty, loadModule],
  );

  /* ---------- save / reload ---------- */
  const save = useCallback(async () => {
    const handles = lgRef.current;
    if (!handles) return;
    setStatus({ kind: "loading", text: "saving…" });
    try {
      const doc = saveFromGraph(handles.graph);
      const r = await fetch(`/api/graph/${module}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(doc),
      });
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(body.error || String(r.status));
      setDirty(false);
      setStatus({
        kind: "ok",
        text: `saved · ${body.nodes} nodes, ${body.edges} edges`,
      });
      refreshHud();
    } catch (e) {
      setStatus({ kind: "err", text: `save failed: ${String(e)}` });
    }
  }, [module]);

  const reload = useCallback(async () => {
    if (dirty) {
      const ok = window.confirm("Discard unsaved changes?");
      if (!ok) return;
    }
    await loadModule(module);
  }, [dirty, module, loadModule]);

  /* ---------- palette add ---------- */
  const addNode = useCallback((typeName: string) => {
    const handles = lgRef.current;
    if (!handles) return;
    const node = LiteGraph.createNode(typeName);
    if (!node) return;
    // Drop near the visible area; LiteGraph's own drag/drop is free.
    const offset = handles.graph._nodes?.length ?? 0;
    node.pos = [60 + (offset % 6) * 40, 60 + Math.floor(offset / 6) * 40];
    handles.graph.add(node);
    handles.graph.setDirtyCanvas(true, true);
    refreshHud();
    setDirty(true);
  }, []);

  const refreshHud = useCallback(() => {
    const handles = lgRef.current;
    if (!handles) return;
    setHud({
      nodes: handles.graph._nodes?.length ?? 0,
      edges: handles.graph.links?.size ?? 0,
    });
  }, []);

  /* ---------- keyboard shortcuts ---------- */
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === "s") {
        ev.preventDefault();
        save();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [save]);

  /* ---------- resize handler (LiteGraph canvas is raster) ---------- */
  useEffect(() => {
    const el = canvasElRef.current;
    const handles = lgRef.current;
    if (!el) return;
    function resize() {
      if (!el) return;
      const dpr = window.devicePixelRatio || 1;
      const { width, height } = el.getBoundingClientRect();
      el.width = Math.max(1, Math.floor(width * dpr));
      el.height = Math.max(1, Math.floor(height * dpr));
      handles?.canvas.resize();
      handles?.canvas.draw(true, true);
    }
    resize();
    const ro = new ResizeObserver(resize);
    ro.observe(el.parentElement as Element);
    return () => ro.disconnect();
  }, [schemas]);

  /* ---------- render ---------- */
  const prefix = MODULE_PREFIX[module];
  const paletteItems = (schemas ?? [])
    .filter((s) => s.type.startsWith(prefix))
    .sort((a, b) => a.type.localeCompare(b.type));

  return (
    <div className="editor-root">
      <header className="editor-topbar">
        <div className="editor-brand">
          <span className="mark">◆</span>
          <span>ML-MAPO</span>
          <span className="sub">graph editor</span>
        </div>
        <nav className="editor-tabs" role="tablist" aria-label="Module tabs">
          {MODULES.map((m) => (
            <button
              key={m}
              className={"editor-tab" + (m === module ? " active" : "")}
              role="tab"
              aria-selected={m === module}
              onClick={() => onSwitchModule(m)}
            >
              {m}
            </button>
          ))}
        </nav>
        <div className="editor-spacer" />
        <div className="editor-actions">
          <button className="editor-btn" onClick={reload} title="Reload graph from disk">
            ↻ Reload
          </button>
          <button
            className="editor-btn primary"
            onClick={save}
            title="Save graph to disk (Ctrl+S)"
          >
            Save
          </button>
          <span
            className={"editor-status" + (status ? " " + status.kind : "")}
            role="status"
            aria-live="polite"
          >
            {status?.text ?? ""}
            {dirty ? " · unsaved" : ""}
          </span>
        </div>
      </header>

      <main className="editor-workspace">
        <aside className="editor-palette" aria-label="Node palette">
          <header className="editor-panel-header">
            <h3>Palette</h3>
            <span className="sub">{module}</span>
          </header>
          <div className="editor-palette-list">
            {paletteItems.map((s) => (
              <button
                key={s.type}
                className="editor-palette-item"
                data-cat={s.category}
                data-type={s.type}
                title={s.doc || s.type}
                onClick={() => addNode(s.type)}
              >
                <span className="type">{s.type.split("/").slice(1).join("/")}</span>
              </button>
            ))}
            {paletteItems.length === 0 && schemas ? (
              <p style={{ color: "var(--text-dim)", padding: "10px 14px", fontSize: 12 }}>
                No registered node types for this module.
              </p>
            ) : null}
          </div>
          <p className="editor-palette-hint">
            Click a node to add. Drag node headers to move, drag from an output
            port to an input port to wire. Right-click a node (or a wire) for
            LiteGraph&apos;s contextual menu. <kbd>Ctrl+S</kbd> saves.
          </p>
        </aside>

        <section className="editor-canvas-wrap">
          <canvas ref={canvasElRef} className="editor-canvas" tabIndex={0} />
          <div className="editor-hud" data-testid="editor-hud">
            {module} · {hud.nodes} nodes · {hud.edges} edges
            {dirty ? " · unsaved" : ""}
          </div>
        </section>
      </main>
    </div>
  );
}
