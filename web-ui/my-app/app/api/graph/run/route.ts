import { NextResponse } from "next/server";

import { PythonCommandError, runGraph } from "@/app/api/graph/_lib/graph-store";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const body = await request.json().catch(() => ({}));
  const ticks = Number(body?.ticks ?? 1);

  try {
    return NextResponse.json(await runGraph(ticks));
  } catch (error) {
    if (error instanceof PythonCommandError) {
      return NextResponse.json(
        {
          error: "graph launch failed",
          ...error.result,
        },
        { status: 500 },
      );
    }
    return NextResponse.json(
      { error: "graph launch failed", detail: String(error) },
      { status: 500 },
    );
  }
}
