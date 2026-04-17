import { NextResponse } from "next/server";

import { MODULES } from "@/lib/types";

export async function GET() {
  return NextResponse.json({ modules: MODULES });
}
