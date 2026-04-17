import { readFile, writeFile } from "node:fs/promises";

import { NextResponse } from "next/server";

import { moduleGraphPath } from "@/lib/paths";
import { MODULES, type GraphDoc, type NodeSchema } from "@/lib/types";

export const dynamic = "force-dynamic";

type Params = { module: string };

function isModule(m: string): m is (typeof MODULES)[number] {
  return (MODULES as readonly string[]).includes(m);
}

export async function GET(
  _request: Request,
  ctx: { params: Promise<Params> },
) {
  const { module } = await ctx.params;
  if (!isModule(module)) {
    return NextResponse.json(
      { error: `unknown module: ${module}` },
      { status: 404 },
    );
  }
  const raw = await readFile(moduleGraphPath(module), "utf8");
  return new NextResponse(raw, {
    status: 200,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export async function PUT(
  request: Request,
  ctx: { params: Promise<Params> },
) {
  const { module } = await ctx.params;
  if (!isModule(module)) {
    return NextResponse.json(
      { error: `unknown module: ${module}` },
      { status: 404 },
    );
  }

  let payload: GraphDoc;
  try {
    payload = (await request.json()) as GraphDoc;
  } catch (e) {
    return NextResponse.json(
      { error: `invalid JSON: ${e}` },
      { status: 400 },
    );
  }

  // Delegate validation + normalization to a helper that reuses the
  // schema cache already populated by /api/graph/schemas.
  try {
    const schemas = await loadSchemas(request.url);
    validate(payload, schemas);
  } catch (e) {
    return NextResponse.json({ error: String(e) }, { status: 400 });
  }

  const normalized = normalize(payload);
  await writeFile(
    moduleGraphPath(module),
    JSON.stringify(normalized, null, 2) + "\n",
    "utf8",
  );
  return NextResponse.json({
    ok: true,
    path: moduleGraphPath(module),
    nodes: normalized.nodes.length,
    edges: normalized.edges.length,
  });
}

/** Fetch schemas from our own API so we share the in-memory cache. */
async function loadSchemas(referer: string): Promise<NodeSchema[]> {
  const url = new URL("/api/graph/schemas", referer);
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`schema fetch failed: ${r.status}`);
  return (await r.json()) as NodeSchema[];
}

function validate(payload: unknown, schemas: NodeSchema[]): void {
  if (!payload || typeof payload !== "object") {
    throw new Error("payload must be a JSON object");
  }
  const p = payload as Partial<GraphDoc>;
  if (!Array.isArray(p.nodes) || !Array.isArray(p.edges)) {
    throw new Error("payload must have nodes[] and edges[]");
  }
  const known = new Set(schemas.map((s) => s.type));
  const ids = new Set<string>();
  for (const n of p.nodes) {
    if (!n || typeof n !== "object") throw new Error("each node must be an object");
    if (typeof n.id !== "string" || !n.id) throw new Error("node.id must be a non-empty string");
    if (ids.has(n.id)) throw new Error(`duplicate node id: ${n.id}`);
    ids.add(n.id);
    if (!known.has(n.type)) throw new Error(`unknown node type: ${n.type}`);
  }
  for (const e of p.edges) {
    if (!e || typeof e !== "object") throw new Error("each edge must be an object");
    for (const k of ["src_node", "src_port", "dst_node", "dst_port"] as const) {
      const v = (e as unknown as Record<string, unknown>)[k];
      if (typeof v !== "string" || !v) throw new Error(`edge missing string field: ${k}`);
    }
    if (!ids.has(e.src_node) || !ids.has(e.dst_node)) {
      throw new Error(`edge references unknown node: ${e.src_node} -> ${e.dst_node}`);
    }
  }
}

function normalize(payload: GraphDoc): GraphDoc {
  return {
    nodes: payload.nodes.map((n) => ({
      id: n.id,
      type: n.type,
      params: { ...(n.params ?? {}) },
      pos: [Number(n.pos?.[0] ?? 0), Number(n.pos?.[1] ?? 0)],
    })),
    edges: payload.edges.map((e) => ({
      src_node: e.src_node,
      src_port: e.src_port,
      dst_node: e.dst_node,
      dst_port: e.dst_port,
    })),
  };
}
