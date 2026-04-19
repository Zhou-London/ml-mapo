import { spawn } from "node:child_process";

import {
  graphPath,
  pythonExecutable,
  repoRoot,
  runtimePath,
} from "@/lib/paths";

export const dynamic = "force-dynamic";

const NODE_EVENT_PREFIX = "__NODE_EVENT__ ";

export async function POST(request: Request) {
  const body = await request.json().catch(() => ({}));
  const ticks = Math.max(1, Math.trunc(Number(body?.ticks ?? 1)));

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();

      function send(event: string, data: unknown) {
        controller.enqueue(
          encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`),
        );
      }

      const proc = spawn(
        pythonExecutable(),
        [
          runtimePath(),
          "--graph",
          graphPath(),
          "--ticks",
          String(ticks),
          "--emit-events",
        ],
        {
          cwd: repoRoot(),
          stdio: ["ignore", "pipe", "pipe"],
          env: { ...process.env, PYTHONUNBUFFERED: "1" },
        },
      );

      const stdoutBuf: string[] = [];
      const stderrBuf: string[] = [];
      let stdoutTail = "";
      let stderrTail = "";

      proc.stdout.setEncoding("utf8");
      proc.stderr.setEncoding("utf8");

      proc.stdout.on("data", (chunk: string) => {
        stdoutTail += chunk;
        const lines = stdoutTail.split("\n");
        stdoutTail = lines.pop() ?? "";
        for (const line of lines) {
          if (line.startsWith(NODE_EVENT_PREFIX)) {
            try {
              const event = JSON.parse(line.slice(NODE_EVENT_PREFIX.length));
              const kind = event.event === "node_start" ? "node_start" : "node_end";
              send(kind, event);
            } catch {
              stdoutBuf.push(line);
            }
          } else {
            stdoutBuf.push(line);
          }
        }
      });

      proc.stderr.on("data", (chunk: string) => {
        stderrTail += chunk;
        const lines = stderrTail.split("\n");
        stderrTail = lines.pop() ?? "";
        stderrBuf.push(...lines);
      });

      proc.on("error", (err) => {
        send("error", { detail: String(err) });
        controller.close();
      });

      proc.on("close", (code) => {
        if (stdoutTail) stdoutBuf.push(stdoutTail);
        if (stderrTail) stderrBuf.push(stderrTail);
        send("done", {
          ok: code === 0,
          code: code ?? -1,
          stdout: stdoutBuf.join("\n"),
          stderr: stderrBuf.join("\n"),
        });
        controller.close();
      });

      const abort = () => {
        if (!proc.killed) proc.kill("SIGTERM");
      };
      request.signal.addEventListener("abort", abort);
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
    },
  });
}
