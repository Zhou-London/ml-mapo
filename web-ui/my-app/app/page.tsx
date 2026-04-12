"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { NavBar } from "@/app/components/nav-bar";

type ModuleName = "data" | "forecast" | "risk" | "opt";
const MODULE_TITLES: Record<ModuleName, string> = {
  data: "Data",
  risk: "Risk",
  forecast: "Forecast",
  opt: "Optimization",
};

type Status = Record<
  ModuleName,
  { running: boolean; pid: number | null }
>;

type Snapshot = {
  module: ModuleName;
  name: string;
  ts: string;
  receivedAt: string;
  data: Record<string, unknown>;
};

// ---------- small render helpers ----------

function fmtNum(n: unknown, digits = 4): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return String(n);
  if (Math.abs(n) >= 1000) return n.toFixed(0);
  return n.toFixed(digits);
}

function fmtPct(n: unknown, digits = 2): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return String(n);
  return `${n.toFixed(digits)}%`;
}

function StaleBadge({ receivedAt }: { receivedAt: string | undefined }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  if (!receivedAt) {
    return <span className="text-xs text-zinc-600">never</span>;
  }
  const age = (now - new Date(receivedAt).getTime()) / 1000;
  const label =
    age < 2 ? "live" : age < 10 ? `${age.toFixed(0)}s ago` : `${age.toFixed(0)}s stale`;
  const cls =
    age < 2
      ? "text-emerald-400"
      : age < 10
      ? "text-zinc-400"
      : "text-amber-400";
  return <span className={`text-xs ${cls}`}>{label}</span>;
}

function KV({ label, value, hint }: { label: string; value: React.ReactNode; hint?: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wide text-zinc-500">
        {label}
      </div>
      <div className="text-sm text-zinc-100 font-mono">{value}</div>
      {hint && <div className="text-[10px] text-zinc-600">{hint}</div>}
    </div>
  );
}

function HealthDot({ state }: { state: "ok" | "warn" | "bad" | "idle" }) {
  const cls = {
    ok: "bg-emerald-400",
    warn: "bg-amber-400",
    bad: "bg-red-500",
    idle: "bg-zinc-600",
  }[state];
  return <span className={`inline-block h-2 w-2 rounded-full ${cls}`} />;
}

function Card({
  title,
  accent,
  running,
  pid,
  receivedAt,
  onStart,
  onStop,
  health,
  children,
}: {
  title: string;
  accent: string;
  running: boolean;
  pid: number | null;
  receivedAt?: string;
  onStart: () => void;
  onStop: () => void;
  health: "ok" | "warn" | "bad" | "idle";
  children: React.ReactNode;
}) {
  return (
    <section
      className={`rounded-lg border ${accent} bg-zinc-950 flex flex-col`}
    >
      <header className="flex items-center justify-between px-3 py-2 border-b border-zinc-800">
        <div className="flex items-center gap-2">
          <HealthDot state={health} />
          <h2 className="font-semibold text-sm">{title}</h2>
          <span className="text-xs text-zinc-500">
            {running ? `pid ${pid}` : "stopped"}
          </span>
          <StaleBadge receivedAt={receivedAt} />
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
      <div className="p-3 flex flex-col gap-3">{children}</div>
    </section>
  );
}

// ---------- type-narrowing helpers ----------

type Asset = {
  symbol: string;
  bars: number;
  first_bar: string;
  last_bar: string;
  last_close: number;
  period_return_pct: number;
  vol_annualized_pct: number;
  nans: number;
};

type FactorStats = {
  name: string;
  information_ratio: number;
  mean: number;
  std: number;
  min: number;
  max: number;
  n: number;
  n_nan: number;
  top: { ticker: string; score: number }[];
  bottom: { ticker: string; score: number }[];
};

// ---------- module-specific panel bodies ----------

function DataBody({ snap }: { snap: Snapshot | undefined }) {
  if (!snap) return <p className="text-xs text-zinc-500">No data yet.</p>;
  const d = snap.data;
  const fetch = (d.fetch ?? {}) as Record<string, number>;
  const window = (d.window ?? {}) as { start?: string; end?: string };
  const assets = ((d.assets as Asset[]) ?? []).slice();

  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <KV label="seq" value={String(d.seq ?? "—")} />
        <KV
          label="window"
          value={`${window.start ?? ""} → ${window.end ?? ""}`}
        />
        <KV label="assets" value={String(d.total_assets ?? 0)} />
        <KV label="total bars" value={String(d.total_bars ?? 0)} />
      </div>
      <div className="grid grid-cols-5 gap-3">
        <KV label="cached" value={String(fetch.cached ?? 0)} />
        <KV label="fetched" value={String(fetch.fetched ?? 0)} />
        <KV label="upserted" value={String(fetch.upserted ?? 0)} />
        <KV
          label="warnings"
          value={
            <span
              className={
                (fetch.warnings ?? 0) > 0 ? "text-amber-400" : ""
              }
            >
              {fetch.warnings ?? 0}
            </span>
          }
        />
        <KV
          label="errors"
          value={
            <span
              className={(fetch.errors ?? 0) > 0 ? "text-red-400" : ""}
            >
              {fetch.errors ?? 0}
            </span>
          }
        />
      </div>
      <div>
        <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
          Asset health ({assets.length})
        </div>
        <div className="max-h-64 overflow-auto rounded border border-zinc-800">
          <table className="w-full text-xs font-mono">
            <thead className="bg-zinc-900 text-zinc-500 sticky top-0">
              <tr>
                <th className="text-left px-2 py-1">symbol</th>
                <th className="text-right px-2 py-1">bars</th>
                <th className="text-right px-2 py-1">last close</th>
                <th className="text-right px-2 py-1">period %</th>
                <th className="text-right px-2 py-1">vol %</th>
                <th className="text-right px-2 py-1">nans</th>
              </tr>
            </thead>
            <tbody>
              {assets.map((a) => {
                const retCls =
                  a.period_return_pct > 0
                    ? "text-emerald-300"
                    : a.period_return_pct < 0
                    ? "text-red-400"
                    : "text-zinc-400";
                const nanCls = a.nans > 0 ? "text-amber-400" : "text-zinc-500";
                return (
                  <tr key={a.symbol} className="border-t border-zinc-900">
                    <td className="px-2 py-0.5 text-zinc-200">{a.symbol}</td>
                    <td className="px-2 py-0.5 text-right text-zinc-400">
                      {a.bars}
                    </td>
                    <td className="px-2 py-0.5 text-right text-zinc-300">
                      {fmtNum(a.last_close, 2)}
                    </td>
                    <td className={`px-2 py-0.5 text-right ${retCls}`}>
                      {fmtNum(a.period_return_pct, 2)}
                    </td>
                    <td className="px-2 py-0.5 text-right text-zinc-300">
                      {fmtNum(a.vol_annualized_pct, 2)}
                    </td>
                    <td className={`px-2 py-0.5 text-right ${nanCls}`}>
                      {a.nans}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function RiskBody({ snap }: { snap: Snapshot | undefined }) {
  if (!snap) return <p className="text-xs text-zinc-500">No data yet.</p>;
  const d = snap.data as Record<string, unknown>;
  const shape = (d.shape ?? [0, 0]) as number[];
  const health = (d.health as string) ?? "unknown";
  const cond = d.condition_number as number | null | undefined;
  const healthCls =
    health === "ok"
      ? "text-emerald-400"
      : health === "ill_conditioned"
      ? "text-amber-400"
      : "text-red-400";
  const topVol = (d.top_vol as { ticker: string; vol_pct: number }[]) ?? [];
  const lowVol = (d.low_vol as { ticker: string; vol_pct: number }[]) ?? [];
  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <KV label="seq" value={String(d.seq ?? "—")} />
        <KV label="factor" value={String(d.factor ?? "—")} />
        <KV label="shape" value={`${shape[0]} × ${shape[1]}`} />
        <KV
          label="health"
          value={<span className={healthCls}>{health}</span>}
        />
      </div>
      <div className="grid grid-cols-4 gap-3">
        <KV
          label="cond number"
          value={
            cond === null || cond === undefined
              ? "∞"
              : fmtNum(cond, 0)
          }
          hint="max/pos-min eigenvalue"
        />
        <KV
          label="neg eigs"
          value={
            <span
              className={
                (d.negative_eigs as number) > 0 ? "text-red-400" : ""
              }
            >
              {String(d.negative_eigs ?? 0)}
            </span>
          }
        />
        <KV
          label="eig min"
          value={fmtNum(d.eig_min, 6)}
          hint={`pos-min ${fmtNum(d.eig_pos_min, 6)}`}
        />
        <KV label="eig max" value={fmtNum(d.eig_max, 6)} />
      </div>
      <div className="grid grid-cols-3 gap-3">
        <KV
          label="vol range"
          value={`${fmtNum(d.vol_annualized_pct_min, 2)}% – ${fmtNum(
            d.vol_annualized_pct_max,
            2,
          )}%`}
          hint="annualized stddev"
        />
        <KV
          label="mean |corr|"
          value={fmtNum(d.off_diag_corr_abs_mean, 4)}
          hint="off-diag average"
        />
        <KV
          label="corr range"
          value={`${fmtNum(d.off_diag_corr_min, 3)} … ${fmtNum(
            d.off_diag_corr_max,
            3,
          )}`}
        />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Top volatility
          </div>
          <ul className="text-xs font-mono">
            {topVol.map((v) => (
              <li
                key={v.ticker}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{v.ticker}</span>
                <span className="text-amber-300">
                  {fmtNum(v.vol_pct, 2)}%
                </span>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Lowest volatility
          </div>
          <ul className="text-xs font-mono">
            {lowVol.map((v) => (
              <li
                key={v.ticker}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{v.ticker}</span>
                <span className="text-emerald-300">
                  {fmtNum(v.vol_pct, 2)}%
                </span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </>
  );
}

function ForecastBody({ snap }: { snap: Snapshot | undefined }) {
  if (!snap) return <p className="text-xs text-zinc-500">No data yet.</p>;
  const d = snap.data as Record<string, unknown>;
  const factors = (d.factors as FactorStats[]) ?? [];
  const combined = (d.combined ?? {}) as FactorStats;
  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <KV label="seq" value={String(d.seq ?? "—")} />
        <KV label="factors" value={String(d.n_factors ?? 0)} />
        <KV label="combined n" value={String(combined.n ?? 0)} />
        <KV
          label="combined nan"
          value={
            <span
              className={(combined.n_nan ?? 0) > 0 ? "text-amber-400" : ""}
            >
              {String(combined.n_nan ?? 0)}
            </span>
          }
        />
      </div>
      <div className="grid grid-cols-4 gap-3">
        <KV label="alpha mean" value={fmtNum(combined.mean, 6)} />
        <KV label="alpha std" value={fmtNum(combined.std, 6)} />
        <KV label="alpha min" value={fmtNum(combined.min, 6)} />
        <KV label="alpha max" value={fmtNum(combined.max, 6)} />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Top alpha (combined)
          </div>
          <ul className="text-xs font-mono">
            {(combined.top ?? []).map((t) => (
              <li
                key={t.ticker}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{t.ticker}</span>
                <span className="text-emerald-300">
                  {fmtNum(t.score, 6)}
                </span>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Bottom alpha
          </div>
          <ul className="text-xs font-mono">
            {(combined.bottom ?? []).map((t) => (
              <li
                key={t.ticker}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{t.ticker}</span>
                <span className="text-red-400">{fmtNum(t.score, 6)}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
      {factors.length > 0 && (
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Per-factor stats
          </div>
          <table className="w-full text-xs font-mono">
            <thead className="text-zinc-500">
              <tr>
                <th className="text-left px-2 py-1">factor</th>
                <th className="text-right px-2 py-1">IR</th>
                <th className="text-right px-2 py-1">mean</th>
                <th className="text-right px-2 py-1">std</th>
                <th className="text-right px-2 py-1">nan</th>
              </tr>
            </thead>
            <tbody>
              {factors.map((f) => (
                <tr key={f.name} className="border-t border-zinc-900">
                  <td className="px-2 py-0.5 text-zinc-200">{f.name}</td>
                  <td className="px-2 py-0.5 text-right text-zinc-300">
                    {fmtNum(f.information_ratio, 2)}
                  </td>
                  <td className="px-2 py-0.5 text-right text-zinc-300">
                    {fmtNum(f.mean, 6)}
                  </td>
                  <td className="px-2 py-0.5 text-right text-zinc-300">
                    {fmtNum(f.std, 6)}
                  </td>
                  <td
                    className={`px-2 py-0.5 text-right ${
                      f.n_nan > 0 ? "text-amber-400" : "text-zinc-500"
                    }`}
                  >
                    {f.n_nan}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

function OptBody({ snap }: { snap: Snapshot | undefined }) {
  if (!snap) return <p className="text-xs text-zinc-500">No data yet.</p>;
  const d = snap.data as Record<string, unknown>;
  const conv = (d.convergence ?? {}) as Record<string, unknown>;
  const port = (d.portfolio ?? {}) as Record<string, number>;
  const metrics = (d.metrics ?? {}) as Record<string, number>;
  const top = (d.top_holdings as { ticker: string; weight: number }[]) ?? [];
  const bottom =
    (d.bottom_holdings as { ticker: string; weight: number }[]) ?? [];
  const converged = conv.converged === true;

  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <KV label="seq" value={String(d.seq ?? "—")} />
        <KV
          label="convergence"
          value={
            <span
              className={converged ? "text-emerald-400" : "text-red-400"}
            >
              {converged ? "yes" : "no"}
            </span>
          }
          hint={`${conv.iterations ?? 0} iters`}
        />
        <KV
          label="objective"
          value={fmtNum(conv.final_objective, 6)}
          hint="-utility"
        />
        <KV
          label="risk aversion"
          value={fmtNum(metrics.risk_aversion, 1)}
        />
      </div>
      <div className="grid grid-cols-4 gap-3">
        <KV label="assets" value={String(port.n_assets ?? 0)} />
        <KV label="nonzero" value={String(port.n_nonzero ?? 0)} />
        <KV
          label="gross / net"
          value={`${fmtNum(port.gross, 3)} / ${fmtNum(port.net, 3)}`}
        />
        <KV
          label="effective N"
          value={fmtNum(port.effective_n, 2)}
          hint={`HHI ${fmtNum(port.herfindahl, 3)}`}
        />
      </div>
      <div className="grid grid-cols-4 gap-3">
        <KV
          label="exp return"
          value={fmtPct((metrics.expected_return ?? 0) * 100, 2)}
        />
        <KV
          label="port vol"
          value={fmtPct((metrics.portfolio_vol_annualized ?? 0) * 100, 2)}
          hint="annualized"
        />
        <KV label="sharpe" value={fmtNum(metrics.sharpe_naive, 3)} />
        <KV
          label="max weight"
          value={fmtNum(port.max_weight, 4)}
        />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Top holdings
          </div>
          <ul className="text-xs font-mono">
            {top.map((h) => (
              <li
                key={h.ticker}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{h.ticker}</span>
                <span className="text-cyan-300">
                  {fmtNum(h.weight * 100, 2)}%
                </span>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Smallest nonzero
          </div>
          <ul className="text-xs font-mono">
            {bottom.map((h) => (
              <li
                key={h.ticker}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{h.ticker}</span>
                <span className="text-zinc-400">
                  {fmtNum(h.weight * 100, 2)}%
                </span>
              </li>
            ))}
          </ul>
        </div>
      </div>
      {!converged && conv.message && (
        <div className="text-xs text-amber-400 border border-amber-900 bg-amber-950/30 px-2 py-1 rounded">
          ⚠ optimizer: {String(conv.message)}
        </div>
      )}
    </>
  );
}

// ---------- page ----------

const ACCENT: Record<ModuleName, string> = {
  data: "border-sky-500",
  risk: "border-amber-500",
  forecast: "border-emerald-500",
  opt: "border-cyan-500",
};

export default function Overview() {
  const [status, setStatus] = useState<Status | null>(null);
  const [snapshots, setSnapshots] = useState<Record<string, Snapshot>>({});
  const [connected, setConnected] = useState(false);

  const refreshStatus = useCallback(async () => {
    try {
      const r = await fetch("/api/modules", { cache: "no-store" });
      const j = await r.json();
      setStatus(j.status);
    } catch {}
  }, []);

  // Load any existing snapshots from the server on mount.
  useEffect(() => {
    (async () => {
      try {
        const r = await fetch("/api/snapshots", { cache: "no-store" });
        const j = (await r.json()) as {
          snapshots: Snapshot[];
          status: Status;
        };
        setStatus(j.status);
        const map: Record<string, Snapshot> = {};
        for (const s of j.snapshots) map[`${s.module}:${s.name}`] = s;
        setSnapshots(map);
      } catch {}
    })();
  }, []);

  // Subscribe to the same SSE stream; filter snapshot events.
  useEffect(() => {
    const es = new EventSource("/api/events");
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.onmessage = (m) => {
      try {
        const e = JSON.parse(m.data) as {
          kind?: string;
          module: ModuleName;
          snapshotName?: string;
          snapshotData?: Record<string, unknown>;
          ts?: string;
          receivedAt: string;
        };
        if (e.kind === "snapshot" && e.snapshotName) {
          const key = `${e.module}:${e.snapshotName}`;
          setSnapshots((prev) => ({
            ...prev,
            [key]: {
              module: e.module,
              name: e.snapshotName!,
              ts: e.ts ?? e.receivedAt,
              receivedAt: e.receivedAt,
              data: e.snapshotData ?? {},
            },
          }));
        }
        // Re-fetch status on system events (start/stop notifications).
        if ((e as unknown as { module: string }).module === "system") {
          refreshStatus();
        }
      } catch {}
    };
    return () => es.close();
  }, [refreshStatus]);

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

  const pick = (module: ModuleName, name: string): Snapshot | undefined =>
    snapshots[`${module}:${name}`];

  const dataSnap = pick("data", "data.cycle");
  const riskSnap = pick("risk", "risk.cov");
  const forecastSnap = pick("forecast", "forecast.alpha");
  const optSnap = pick("opt", "opt.solution");

  const health = useMemo(() => {
    const h: Record<ModuleName, "ok" | "warn" | "bad" | "idle"> = {
      data: "idle",
      risk: "idle",
      forecast: "idle",
      opt: "idle",
    };
    if (dataSnap) {
      const f = (dataSnap.data.fetch ?? {}) as Record<string, number>;
      h.data =
        (f.errors ?? 0) > 0
          ? "bad"
          : (f.warnings ?? 0) > 0
          ? "warn"
          : "ok";
    }
    if (riskSnap) {
      const hs = riskSnap.data.health as string;
      h.risk = hs === "ok" ? "ok" : hs === "ill_conditioned" ? "warn" : "bad";
    }
    if (forecastSnap) {
      const c = (forecastSnap.data.combined ?? {}) as { n_nan?: number };
      h.forecast = (c.n_nan ?? 0) > 0 ? "warn" : "ok";
    }
    if (optSnap) {
      const c = (optSnap.data.convergence ?? {}) as { converged?: boolean };
      h.opt = c.converged ? "ok" : "bad";
    }
    return h;
  }, [dataSnap, riskSnap, forecastSnap, optSnap]);

  return (
    <div className="min-h-screen bg-black text-zinc-200 font-sans p-4">
      <NavBar
        status={status}
        connected={connected}
        onStartAll={() => act("start_all")}
        onStopAll={() => act("stop_all")}
      />

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
        <Card
          title={MODULE_TITLES.data}
          accent={ACCENT.data}
          running={status?.data?.running ?? false}
          pid={status?.data?.pid ?? null}
          receivedAt={dataSnap?.receivedAt}
          onStart={() => act("start", "data")}
          onStop={() => act("stop", "data")}
          health={health.data}
        >
          <DataBody snap={dataSnap} />
        </Card>

        <Card
          title={MODULE_TITLES.risk}
          accent={ACCENT.risk}
          running={status?.risk?.running ?? false}
          pid={status?.risk?.pid ?? null}
          receivedAt={riskSnap?.receivedAt}
          onStart={() => act("start", "risk")}
          onStop={() => act("stop", "risk")}
          health={health.risk}
        >
          <RiskBody snap={riskSnap} />
        </Card>

        <Card
          title={MODULE_TITLES.forecast}
          accent={ACCENT.forecast}
          running={status?.forecast?.running ?? false}
          pid={status?.forecast?.pid ?? null}
          receivedAt={forecastSnap?.receivedAt}
          onStart={() => act("start", "forecast")}
          onStop={() => act("stop", "forecast")}
          health={health.forecast}
        >
          <ForecastBody snap={forecastSnap} />
        </Card>

        <Card
          title={MODULE_TITLES.opt}
          accent={ACCENT.opt}
          running={status?.opt?.running ?? false}
          pid={status?.opt?.pid ?? null}
          receivedAt={optSnap?.receivedAt}
          onStart={() => act("start", "opt")}
          onStop={() => act("stop", "opt")}
          health={health.opt}
        >
          <OptBody snap={optSnap} />
        </Card>
      </div>

      <p className="text-[10px] text-zinc-600 mt-4">
        Each panel shows the latest <code>snapshot</code> event emitted by its
        module. For the full log stream, see the{" "}
        <a href="/raw" className="underline">
          Raw Output
        </a>{" "}
        page.
      </p>
    </div>
  );
}
