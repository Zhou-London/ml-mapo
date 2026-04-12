import { manager } from "@/app/lib/process-manager";

export const dynamic = "force-dynamic";

// Server-Sent Events stream of structured log events from every module.
// On connect we replay the current backlog, then forward each new event.
export async function GET() {
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      const send = (data: unknown) => {
        try {
          controller.enqueue(
            encoder.encode(`data: ${JSON.stringify(data)}\n\n`),
          );
        } catch {
          // Client disconnected; the cleanup below will unsubscribe.
        }
      };

      // Replay backlog so late-joining browsers see recent history.
      for (const e of manager.allBuffers()) send(e);

      const unsubscribe = manager.subscribe(send);

      // Heartbeat so intermediate proxies don't close an idle connection.
      const heartbeat = setInterval(() => {
        try {
          controller.enqueue(encoder.encode(": ping\n\n"));
        } catch {
          clearInterval(heartbeat);
        }
      }, 15_000);

      // @ts-expect-error - cancel hook is set so we can clean up in close()
      controller._mapoCleanup = () => {
        unsubscribe();
        clearInterval(heartbeat);
      };
    },
    cancel(_reason) {
      // @ts-expect-error - matching hook set in start()
      this._mapoCleanup?.();
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    },
  });
}
