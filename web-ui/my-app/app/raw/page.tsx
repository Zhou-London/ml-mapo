"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { NavBar } from "@/app/components/nav-bar";

type ModuleName = "data" | "forecast" | "risk" | "opt";
const MODULES: ModuleName[] = ["data", "risk", "forecast", "opt"];
const MODULE_TITLES: Record<ModuleName, string> = {
  data: "Data",
  risk: "Risk",
  forecast: "Forecast",
  opt: "Optimization",
};
const MODULE_ACCENT: Record<ModuleName, string> = {
  data: "border-sky-500 text-sky-300",
  risk: "border-amber-500 text-amber-300",
  forecast: "border-emerald-500 text-emerald-300",
  opt: "border-cyan-500 text-cyan-300",
};

type LogEvent = {
  id: number;
  receivedAt: string;
  module: ModuleName | "system";
  level: string;
  stage: string;
  msg: string;
  ts?: string;
  fields: Record<string, unknown>;
  block?: string[];
  raw?: string;
};

type Status = Record<ModuleName, { running: boolean; pid: number | null }>;

const BUFFER_CAP = 400;

const LEVEL_STYLE: Record<string, string> = {
  DEBUG: "text-zinc-500",
  INFO: "text-sky-300",
  WARN: "text-amber-300",
  ERROR: "text-red-400",
  FATAL: "text-red-500 font-bold",
  RAW: "text-zinc-400 italic",
};

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return String(v);
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function formatTime(ts?: string, fallback?: string): string {
  const src = ts ?? fallback;
  if (!src) return "";
  // Extract HH:MM:SS.mmm from ISO string for compactness.
  const m = src.match(/T(\d{2}:\d{2}:\d{2})(\.\d{1,3})?/);
  return m ? m[1] + (m[2] ?? "") : src;
}

function EventRow({ e }: { e: LogEvent }) {
  const levelClass = LEVEL_STYLE[e.level] ?? "text-zinc-300";
  const fieldEntries = Object.entries(e.fields ?? {});
  return (
    <div className="py-0.5 border-b border-zinc-900 font-mono text-[11px] leading-relaxed">
      <div className="flex flex-wrap items-baseline gap-x-2">
        <span className="text-zinc-600">{formatTime(e.ts, e.receivedAt)}</span>
        <span className={`${levelClass} w-10 shrink-0`}>{e.level}</span>
        {e.stage && (
          <span className="text-zinc-500">[{e.stage}]</span>
        )}
        <span className="text-zinc-200">{e.msg}</span>
      </div>
      {fieldEntries.length > 0 && (
        <div className="pl-12 text-zinc-400 break-all">
          {fieldEntries.map(([k, v]) => (
            <span key={k} className="mr-3">
              <span className="text-zinc-500">{k}=</span>
              <span>{formatValue(v)}</span>
            </span>
          ))}
        </div>
      )}
      {e.block && e.block.length > 0 && (
        <pre className="pl-12 text-zinc-400 whitespace-pre">
          {e.block.join("\n")}
        </pre>
      )}
    </div>
  );
}

function ModulePanel({
  name,
  events,
  running,
  pid,
  onStart,
  onStop,
}: {
  name: ModuleName;
  events: LogEvent[];
  running: boolean;
  pid: number | null;
  onStart: () => void;
  onStop: () => void;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [autoscroll, setAutoscroll] = useState(true);

  useEffect(() => {
    if (!autoscroll || !scrollRef.current) return;
    scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [events, autoscroll]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    setAutoscroll(nearBottom);
  };

  return (
    <section
      className={`flex flex-col rounded-lg border bg-zinc-950 ${MODULE_ACCENT[name]} min-h-[320px]`}
    >
      <header className="flex items-center justify-between px-3 py-2 border-b border-zinc-800">
        <div className="flex items-center gap-2">
          <span
            className={`inline-block h-2 w-2 rounded-full ${
              running ? "bg-emerald-400" : "bg-zinc-600"
            }`}
          />
          <h2 className="font-semibold text-sm">{MODULE_TITLES[name]}</h2>
          <span className="text-xs text-zinc-500">
            {running ? `pid ${pid}` : "stopped"}
          </span>
        </div>
        <div className="flex gap-1">
          <button
            onClick={onStart}
            disabled={running}
            className="px-2 py-0.5 text-xs rounded border border-zinc-700 hover:bg-zinc-800 disabled:opacity-30 disabled:cursor-not-allowed"
          >
            Start
          </button>
          <button
            onClick={onStop}
            disabled={!running}
            className="px-2 py-0.5 text-xs rounded border border-zinc-700 hover:bg-zinc-800 disabled:opacity-30 disabled:cursor-not-allowed"
          >
            Stop
          </button>
        </div>
      </header>
      <div
        ref={scrollRef}
        onScroll={onScroll}
        className="flex-1 overflow-y-auto px-2 py-1 bg-black/40 max-h-[calc(100vh-260px)]"
      >
        {events.length === 0 ? (
          <p className="text-xs text-zinc-600 p-2">No events yet.</p>
        ) : (
          events.map((e) => <EventRow key={e.id} e={e} />)
        )}
      </div>
    </section>
  );
}

function SystemStrip({ events }: { events: LogEvent[] }) {
  const recent = events.slice(-5).reverse();
  return (
    <div className="rounded border border-zinc-800 bg-zinc-950 px-3 py-2 text-[11px] font-mono">
      <div className="text-zinc-500 mb-1">System</div>
      {recent.length === 0 ? (
        <div className="text-zinc-600">Idle.</div>
      ) : (
        recent.map((e) => (
          <div key={e.id} className="text-zinc-300">
            <span className="text-zinc-600 mr-2">
              {formatTime(e.ts, e.receivedAt)}
            </span>
            <span
              className={`mr-2 ${LEVEL_STYLE[e.level] ?? "text-zinc-300"}`}
            >
              {e.level}
            </span>
            {e.msg}
            {Object.entries(e.fields ?? {}).map(([k, v]) => (
              <span key={k} className="ml-3 text-zinc-500">
                {k}=<span className="text-zinc-400">{formatValue(v)}</span>
              </span>
            ))}
          </div>
        ))
      )}
    </div>
  );
}

export default function Home() {
  const [events, setEvents] = useState<LogEvent[]>([]);
  const [status, setStatus] = useState<Status | null>(null);
  const [connected, setConnected] = useState(false);

  const refreshStatus = useCallback(async () => {
    try {
      const r = await fetch("/api/modules", { cache: "no-store" });
      const j = await r.json();
      setStatus(j.status);
    } catch {
      // Ignore transient fetch errors during HMR.
    }
  }, []);

  useEffect(() => {
    refreshStatus();
    const es = new EventSource("/api/events");
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.onmessage = (m) => {
      try {
        const e = JSON.parse(m.data) as LogEvent;
        setEvents((prev) => {
          const next = prev.length >= BUFFER_CAP * 5 ? prev.slice(-BUFFER_CAP * 4) : prev;
          return [...next, e];
        });
        if (e.module === "system") refreshStatus();
      } catch {
        // Bad event payload, skip.
      }
    };
    return () => es.close();
  }, [refreshStatus]);

  const byModule = useMemo(() => {
    const grouped: Record<ModuleName, LogEvent[]> = {
      data: [],
      forecast: [],
      risk: [],
      opt: [],
    };
    const sys: LogEvent[] = [];
    for (const e of events) {
      if (e.module === "system") sys.push(e);
      else if (e.module in grouped) grouped[e.module].push(e);
    }
    for (const m of MODULES) {
      if (grouped[m].length > BUFFER_CAP) {
        grouped[m] = grouped[m].slice(-BUFFER_CAP);
      }
    }
    return { grouped, sys };
  }, [events]);

  const act = useCallback(
    async (action: string, module?: ModuleName) => {
      await fetch("/api/modules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, module }),
      });
      refreshStatus();
    },
    [refreshStatus],
  );

  return (
    <div className="min-h-screen bg-black text-zinc-200 font-sans p-4">
      <NavBar
        status={status}
        connected={connected}
        onStartAll={() => act("start_all")}
        onStopAll={() => act("stop_all")}
        onClear={() => setEvents([])}
      />

      <div className="mb-3">
        <SystemStrip events={byModule.sys} />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
        {MODULES.map((m) => (
          <ModulePanel
            key={m}
            name={m}
            events={byModule.grouped[m]}
            running={status?.[m]?.running ?? false}
            pid={status?.[m]?.pid ?? null}
            onStart={() => act("start", m)}
            onStop={() => act("stop", m)}
          />
        ))}
      </div>
    </div>
  );
}
