import { NextResponse } from "next/server";

import {
  PythonCommandError,
  readGraph,
  writeGraph,
} from "@/app/api/graph/_lib/graph-store";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    return NextResponse.json(await readGraph());
  } catch (error) {
    return NextResponse.json(
      {
        error: "graph read failed",
        detail: error instanceof Error ? error.message : String(error),
      },
      { status: 500 },
    );
  }
}

export async function PUT(request: Request) {
  try {
    const doc = await writeGraph(await request.json());
    return NextResponse.json({
      ok: true,
      nodes: doc.nodes.length,
      edges: doc.edges.length,
    });
  } catch (error) {
    const detail =
      error instanceof PythonCommandError || error instanceof Error
        ? error.message
        : String(error);
    return NextResponse.json(
      { error: "graph save failed", detail },
      { status: 400 },
    );
  }
}
