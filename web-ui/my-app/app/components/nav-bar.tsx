"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

type Status = Record<
  "data" | "forecast" | "risk" | "opt",
  { running: boolean; pid: number | null }
>;

export function NavBar({
  status,
  connected,
  onStartAll,
  onStopAll,
  onClear,
}: {
  status: Status | null;
  connected: boolean;
  onStartAll: () => void;
  onStopAll: () => void;
  onClear?: () => void;
}) {
  const pathname = usePathname();
  const anyRunning = status
    ? Object.values(status).some((s) => s.running)
    : false;
  const tab = (href: string, label: string) => {
    const active = pathname === href;
    return (
      <Link
        href={href}
        className={`px-3 py-1 rounded text-sm ${
          active
            ? "bg-zinc-800 text-zinc-100"
            : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-900"
        }`}
      >
        {label}
      </Link>
    );
  };

  return (
    <header className="flex flex-wrap items-center justify-between gap-3 mb-4">
      <div className="flex items-center gap-3">
        <div>
          <h1 className="text-lg font-semibold">MAPO Pipeline Dashboard</h1>
          <p className="text-xs text-zinc-500">
            <span
              className={connected ? "text-emerald-400" : "text-amber-400"}
            >
              {connected ? "● connected" : "● reconnecting"}
            </span>
          </p>
        </div>
        <nav className="flex gap-1 ml-2">
          {tab("/", "Overview")}
          {tab("/trace", "Trace")}
          {tab("/raw", "Raw Output")}
        </nav>
      </div>
      <div className="flex gap-2">
        <button
          onClick={onStartAll}
          className="px-3 py-1 rounded border border-emerald-600 text-emerald-300 hover:bg-emerald-900/40 text-sm"
        >
          Start All
        </button>
        <button
          onClick={onStopAll}
          disabled={!anyRunning}
          className="px-3 py-1 rounded border border-red-600 text-red-300 hover:bg-red-900/40 text-sm disabled:opacity-30 disabled:cursor-not-allowed"
        >
          Stop All
        </button>
        {onClear && (
          <button
            onClick={onClear}
            className="px-3 py-1 rounded border border-zinc-700 text-zinc-300 hover:bg-zinc-800 text-sm"
          >
            Clear
          </button>
        )}
      </div>
    </header>
  );
}
