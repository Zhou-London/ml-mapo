import { NextResponse } from "next/server";
import { manager } from "@/app/lib/process-manager";

export const dynamic = "force-dynamic";

// Latest state sample from each module, grouped by snapshot name.
// The Overview page calls this on mount; for live updates it watches the
// SSE /api/events stream and filters to `kind === "snapshot"` events.
export async function GET() {
  return NextResponse.json({
    snapshots: manager.getSnapshots(),
    status: manager.status(),
  });
}
