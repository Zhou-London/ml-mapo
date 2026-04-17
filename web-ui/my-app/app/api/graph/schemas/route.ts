import { spawn } from "node:child_process";

import { NextResponse } from "next/server";

import type { NodeSchema } from "@/lib/types";
import { graphCliPath, pythonExecutable } from "@/lib/paths";

// This handler shells out to the Python prototype; never try to cache it at
// build time. With Next 16 GET handlers default to dynamic but being explicit
// prevents surprises if the route is moved under `use cache`.
export const dynamic = "force-dynamic";

let CACHE: NodeSchema[] | null = null;
let INFLIGHT: Promise<NodeSchema[]> | null = null;

function runPython(args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const proc = spawn(pythonExecutable(), args, {
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    proc.stdout.on("data", (b) => stdout.push(b));
    proc.stderr.on("data", (b) => stderr.push(b));
    proc.on("error", reject);
    proc.on("close", (code) => {
      if (code === 0) resolve(Buffer.concat(stdout).toString("utf8"));
      else {
        const err = Buffer.concat(stderr).toString("utf8") || `exit ${code}`;
        reject(new Error(err));
      }
    });
  });
}

async function loadSchemas(): Promise<NodeSchema[]> {
  const raw = await runPython([graphCliPath(), "schemas"]);
  return JSON.parse(raw) as NodeSchema[];
}

export async function GET(request: Request) {
  const refresh = new URL(request.url).searchParams.get("refresh");
  const force = refresh && refresh !== "0" && refresh !== "false";
  if (force) CACHE = null;

  if (CACHE) return NextResponse.json(CACHE);
  if (!INFLIGHT) {
    INFLIGHT = loadSchemas().finally(() => {
      INFLIGHT = null;
    });
  }
  try {
    CACHE = await INFLIGHT;
    return NextResponse.json(CACHE);
  } catch (e) {
    return NextResponse.json(
      { error: "schema loader failed", detail: String(e) },
      { status: 500 },
    );
  }
}
