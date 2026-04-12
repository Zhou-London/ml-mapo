import { NextRequest, NextResponse } from "next/server";
import { MODULE_NAMES, ModuleName, manager } from "@/app/lib/process-manager";

export const dynamic = "force-dynamic";

export async function GET() {
  return NextResponse.json({ status: manager.status() });
}

type Action = "start" | "stop" | "start_all" | "stop_all";

export async function POST(req: NextRequest) {
  const body = (await req.json().catch(() => ({}))) as {
    action?: Action;
    module?: ModuleName;
  };
  const action = body.action;
  if (!action) {
    return NextResponse.json({ error: "missing action" }, { status: 400 });
  }
  switch (action) {
    case "start_all":
      await manager.startAll();
      break;
    case "stop_all":
      await manager.stopAll();
      break;
    case "start":
    case "stop": {
      const m = body.module;
      if (!m || !MODULE_NAMES.includes(m)) {
        return NextResponse.json(
          { error: "invalid module" },
          { status: 400 },
        );
      }
      if (action === "start") await manager.start(m);
      else await manager.stop(m);
      break;
    }
    default:
      return NextResponse.json({ error: "unknown action" }, { status: 400 });
  }
  return NextResponse.json({ status: manager.status() });
}
