import { NextResponse } from "next/server";

import { loadSchemas } from "@/app/api/graph/_lib/graph-store";

// This handler shells out to the Python prototype; never try to cache it at
// build time. With Next 16 GET handlers default to dynamic but being explicit
// prevents surprises if the route is moved under `use cache`.
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const refresh = new URL(request.url).searchParams.get("refresh");
  const force = refresh !== null && refresh !== "0" && refresh !== "false";
  try {
    return NextResponse.json(await loadSchemas(force));
  } catch (e) {
    return NextResponse.json(
      { error: "schema loader failed", detail: String(e) },
      { status: 500 },
    );
  }
}
