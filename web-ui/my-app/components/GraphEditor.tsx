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
      // LiteGraph's own telemetry overlay (T/I/N/V/FPS) collides with the
      // custom HUD in the corner — suppress it.
      canvas.show_info = false;
      // Match LiteGraph's own background to our theme so the canvas area
      // reads as one continuous panel instead of two shades of grey.
      canvas.clear_background_color = "#15181c";
      // LGraphCanvas normally renders into a separate bgcanvas and then
      // drawImage()s it to the front, scaling by devicePixelRatio in the
      // process. That scaling only composes ~1/dpr² of the visible area on
      // retina, producing the "mask rectangle" + trails the user saw.
      // Pointing bgcanvas at the same element short-circuits that path —
      // LiteGraph renders directly onto the front context.
      canvas.bgcanvas = canvas.canvas;
      canvas.bgctx = canvas.ctx;
      // Default wheel delta is ~1.1; bump so trackpad pinch/zoom doesn't
      // feel sluggish.
      canvas.zoom_speed = 1.15;
      canvas.ds.min_scale = 0.2;
      canvas.ds.max_scale = 3;
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
      // Fit the camera after the canvas raster has a real size. A single
      // RAF is often too early (ResizeObserver hasn't fired yet), so poll
      // a few frames until we see a non-trivial width.
      scheduleFit(lgRef);
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

  const fitView = useCallback(() => {
    const handles = lgRef.current;
    if (!handles) return;
    fitCanvasToGraph(handles.canvas, handles.graph);
  }, []);

  /* ---------- resize handler ----------
   * With bgcanvas aliased to the front canvas, LiteGraph renders once
   * and applies `setTransform(dpr)` before `ds.toCanvasContext` — so
   * the raster MUST be DPR-scaled for content to cover the full area.
   * CSS keeps `width:100% height:100%` so the browser rescales the
   * DPR-sized raster back to the CSS footprint, giving crisp retina
   * text without the compositor mask artifact.
   */
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

  /* ---------- render ---------- */
  const prefix = MODULE_PREFIX[module];
  const paletteItems = (schemas ?? [])
    .filter((s) => s.type.startsWith(prefix))
    .sort((a, b) => a.type.localeCompare(b.type));

  return (
    <div className="editor-root">
      <header className="editor-topbar">
        <div className="editor-brand">
          <span className="mark" aria-hidden="true" />
          <span className="name">ML-MAPO</span>
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
          <button
            className="editor-btn"
            onClick={fitView}
            title="Fit graph to view"
            aria-label="Fit view"
          >
            Fit
          </button>
          <button className="editor-btn" onClick={reload} title="Reload graph from disk">
            Reload
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

      <main className="editor-workspace">
        <aside className="editor-palette" aria-label="Node palette">
          <header className="editor-panel-header">
            <h3>Nodes</h3>
            <span className="count">{paletteItems.length}</span>
          </header>
          <div className="editor-palette-list">
            {paletteItems.map((s) => {
              const label = s.type.split("/").slice(1).join("/") || s.type;
              return (
                <button
                  key={s.type}
                  className="editor-palette-item"
                  data-cat={s.category}
                  data-type={s.type}
                  title={s.doc || s.type}
                  onClick={() => addNode(s.type)}
                >
                  <span className="swatch" aria-hidden="true" />
                  <span className="type">{label}</span>
                  <span className="plus" aria-hidden="true">+</span>
                </button>
              );
            })}
            {paletteItems.length === 0 && schemas ? (
              <p className="editor-palette-empty">
                No registered node types for this module.
              </p>
            ) : null}
          </div>
          <footer className="editor-palette-hint">
            <span>Click to add · drag ports to wire</span>
            <span>
              <kbd>⌘S</kbd> save
            </span>
          </footer>
        </aside>

        <section className="editor-canvas-wrap">
          <canvas ref={canvasElRef} className="editor-canvas" tabIndex={0} />
          <div className="editor-canvas-meta" aria-hidden="true">
            {module} · {hud.nodes} nodes · {hud.edges} edges
          </div>
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

/**
 * Center and zoom the canvas so the full graph fits. We compute a padded
 * bounding box of all nodes and defer to LGraphCanvas' own `fitToBounds`
 * helper, which handles DPR and visible-area math for us.
 */
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
  for (const n of nodes) {
    const [x, y] = (n.pos as [number, number]) ?? [0, 0];
    const [w, h] = (n.size as [number, number]) ?? [120, 60];
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
  // zoom=0.9 → leaves ~10% margin vs. a tight fit.
  canvas.ds.fitToBounds(bounds, { zoom: 0.9 });
  canvas.setDirty(true, true);
}
