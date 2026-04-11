"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { NavBar } from "@/app/components/nav-bar";

type ModuleName = "data" | "forecast" | "risk" | "opt";

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

// ---------- per-asset trace shapes ----------

type DataAssetTrace = {
  bars: number;
  first_bar: string;
  last_bar: string;
  first_close: number;
  last_close: number;
  n_log_returns: number;
  log_return_last: number;
  log_return_mean_daily: number;
  log_return_std_daily: number;
  return_annualized_pct: number;
  vol_annualized_pct: number;
};

type ForecastFactorRow = {
  name: string;
  raw_score: number;
  factor_mean: number;
  factor_std: number;
  z_score: number;
  ir_weight: number;
  contribution: number;
};

type ForecastAssetTrace = {
  factors: ForecastFactorRow[];
  combined_z: number;
  avg_magnitude: number;
  alpha: number;
};

type RiskAssetTrace = {
  variance: number;
  vol_annualized_pct: number;
  mean_corr_others: number;
  max_corr_others: number;
  min_corr_others: number;
  top_corr: { symbol: string; corr: number }[];
  bottom_corr: { symbol: string; corr: number }[];
};

type OptAssetTrace = {
  alpha_input: number;
  sigma_self: number;
  vol_annualized_pct: number;
  final_weight: number;
  constraint_status: "interior" | "at_lower_bound" | "at_upper_bound";
  marginal_utility: number;
  contribution_to_return: number;
  contribution_to_variance: number;
};

// ---------- formatting helpers ----------

function fmtNum(n: unknown, digits = 4): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return String(n);
  if (Math.abs(n) >= 1000) return n.toFixed(0);
  return n.toFixed(digits);
}

function fmtPct(n: unknown, digits = 2): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return String(n);
  return `${n.toFixed(digits)}%`;
}

function fmtSigned(n: number, digits = 4): string {
  const s = n.toFixed(digits);
  return n > 0 ? `+${s}` : s;
}

function KV({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: React.ReactNode;
  hint?: string;
  tone?: "pos" | "neg" | "warn" | "neutral";
}) {
  const toneCls = {
    pos: "text-emerald-300",
    neg: "text-red-400",
    warn: "text-amber-300",
    neutral: "text-zinc-100",
  }[tone ?? "neutral"];
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wide text-zinc-500">
        {label}
      </div>
      <div className={`text-sm font-mono ${toneCls}`}>{value}</div>
      {hint && <div className="text-[10px] text-zinc-600">{hint}</div>}
    </div>
  );
}

function StepCard({
  title,
  accent,
  subtitle,
  children,
}: {
  title: string;
  accent: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <section
      className={`rounded-lg border ${accent} bg-zinc-950 flex flex-col`}
    >
      <header className="flex items-baseline gap-2 px-3 py-2 border-b border-zinc-800">
        <h2 className="font-semibold text-sm">{title}</h2>
        {subtitle && (
          <span className="text-xs text-zinc-500">{subtitle}</span>
        )}
      </header>
      <div className="p-3 flex flex-col gap-3">{children}</div>
    </section>
  );
}

// ---------- module trace bodies ----------

function DataTraceBody({ trace }: { trace: DataAssetTrace | undefined }) {
  if (!trace) return <p className="text-xs text-zinc-500">No trace yet.</p>;
  const rt = trace.return_annualized_pct;
  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <KV label="bars" value={String(trace.bars)} />
        <KV
          label="window"
          value={`${trace.first_bar} → ${trace.last_bar}`}
        />
        <KV label="first close" value={fmtNum(trace.first_close, 2)} />
        <KV label="last close" value={fmtNum(trace.last_close, 2)} />
      </div>
      <div className="grid grid-cols-4 gap-3">
        <KV label="log return last" value={fmtSigned(trace.log_return_last, 6)} />
        <KV label="mean daily" value={fmtSigned(trace.log_return_mean_daily, 6)} />
        <KV label="std daily" value={fmtNum(trace.log_return_std_daily, 6)} />
        <KV label="# returns" value={String(trace.n_log_returns)} />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <KV
          label="annualized return"
          value={fmtPct(rt, 2)}
          hint="μ·252"
          tone={rt > 0 ? "pos" : rt < 0 ? "neg" : "neutral"}
        />
        <KV
          label="annualized vol"
          value={fmtPct(trace.vol_annualized_pct, 2)}
          hint="σ·√252"
        />
      </div>
    </>
  );
}

function ForecastTraceBody({
  trace,
}: {
  trace: ForecastAssetTrace | undefined;
}) {
  if (!trace) return <p className="text-xs text-zinc-500">No trace yet.</p>;
  return (
    <>
      <div>
        <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
          Per-factor math (raw → z → contribution)
        </div>
        <table className="w-full text-xs font-mono">
          <thead className="text-zinc-500">
            <tr>
              <th className="text-left px-2 py-1">factor</th>
              <th className="text-right px-2 py-1">raw</th>
              <th className="text-right px-2 py-1">μ</th>
              <th className="text-right px-2 py-1">σ</th>
              <th className="text-right px-2 py-1">z</th>
              <th className="text-right px-2 py-1">weight</th>
              <th className="text-right px-2 py-1">contrib</th>
            </tr>
          </thead>
          <tbody>
            {trace.factors.map((f) => {
              const z = f.z_score;
              const zCls =
                z > 0.1
                  ? "text-emerald-300"
                  : z < -0.1
                  ? "text-red-400"
                  : "text-zinc-400";
              return (
                <tr key={f.name} className="border-t border-zinc-900">
                  <td className="px-2 py-0.5 text-zinc-200">{f.name}</td>
                  <td className="px-2 py-0.5 text-right text-zinc-300">
                    {fmtSigned(f.raw_score, 4)}
                  </td>
                  <td className="px-2 py-0.5 text-right text-zinc-500">
                    {fmtSigned(f.factor_mean, 4)}
                  </td>
                  <td className="px-2 py-0.5 text-right text-zinc-500">
                    {fmtNum(f.factor_std, 4)}
                  </td>
                  <td className={`px-2 py-0.5 text-right ${zCls}`}>
                    {fmtSigned(f.z_score, 4)}
                  </td>
                  <td className="px-2 py-0.5 text-right text-zinc-400">
                    {fmtNum(f.ir_weight, 3)}
                  </td>
                  <td className={`px-2 py-0.5 text-right ${zCls}`}>
                    {fmtSigned(f.contribution, 4)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="grid grid-cols-3 gap-3">
        <KV
          label="combined z"
          value={fmtSigned(trace.combined_z, 4)}
          hint="Σ contributions"
          tone={
            trace.combined_z > 0
              ? "pos"
              : trace.combined_z < 0
              ? "neg"
              : "neutral"
          }
        />
        <KV
          label="avg magnitude"
          value={fmtNum(trace.avg_magnitude, 4)}
          hint="rescale factor"
        />
        <KV
          label="final alpha"
          value={fmtSigned(trace.alpha, 6)}
          hint="combined_z × avg_magnitude"
          tone={trace.alpha > 0 ? "pos" : trace.alpha < 0 ? "neg" : "neutral"}
        />
      </div>
    </>
  );
}

function RiskTraceBody({ trace }: { trace: RiskAssetTrace | undefined }) {
  if (!trace) return <p className="text-xs text-zinc-500">No trace yet.</p>;
  return (
    <>
      <div className="grid grid-cols-3 gap-3">
        <KV
          label="own variance"
          value={fmtNum(trace.variance, 6)}
          hint="Σ[i,i]"
        />
        <KV
          label="annualized vol"
          value={fmtPct(trace.vol_annualized_pct, 2)}
          hint="√Σ[i,i]·100"
        />
        <KV
          label="mean corr others"
          value={fmtNum(trace.mean_corr_others, 4)}
          hint={`${fmtNum(trace.min_corr_others, 3)} … ${fmtNum(
            trace.max_corr_others,
            3,
          )}`}
        />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Top correlates
          </div>
          <ul className="text-xs font-mono">
            {trace.top_corr.map((c) => (
              <li
                key={c.symbol}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{c.symbol}</span>
                <span className="text-amber-300">
                  {fmtSigned(c.corr, 3)}
                </span>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
            Least correlated / hedges
          </div>
          <ul className="text-xs font-mono">
            {trace.bottom_corr.map((c) => (
              <li
                key={c.symbol}
                className="flex justify-between border-b border-zinc-900 py-0.5"
              >
                <span className="text-zinc-300">{c.symbol}</span>
                <span className="text-emerald-300">
                  {fmtSigned(c.corr, 3)}
                </span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </>
  );
}

function OptTraceBody({
  trace,
  riskAversion,
}: {
  trace: OptAssetTrace | undefined;
  riskAversion: number | undefined;
}) {
  if (!trace) return <p className="text-xs text-zinc-500">No trace yet.</p>;
  const w = trace.final_weight;
  const wCls = w > 0.01 ? "pos" : w < -0.01 ? "neg" : "neutral";
  const statusCls = {
    interior: "text-emerald-300",
    at_lower_bound: "text-amber-300",
    at_upper_bound: "text-amber-300",
  }[trace.constraint_status] ?? "text-zinc-300";
  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <KV
          label="α input"
          value={fmtSigned(trace.alpha_input, 6)}
          hint="from forecast.trace"
          tone={
            trace.alpha_input > 0
              ? "pos"
              : trace.alpha_input < 0
              ? "neg"
              : "neutral"
          }
        />
        <KV
          label="σ²"
          value={fmtNum(trace.sigma_self, 6)}
          hint="from risk.trace"
        />
        <KV
          label="λ (risk aversion)"
          value={fmtNum(riskAversion ?? NaN, 1)}
        />
        <KV
          label="final weight"
          value={fmtPct(w * 100, 3)}
          tone={wCls}
        />
      </div>
      <div className="grid grid-cols-3 gap-3">
        <KV
          label="constraint"
          value={
            <span className={statusCls}>{trace.constraint_status}</span>
          }
          hint={
            trace.constraint_status === "interior"
              ? "free, gradient ≈ 0"
              : "bound is binding"
          }
        />
        <KV
          label="∂U/∂w"
          value={fmtSigned(trace.marginal_utility, 4)}
          hint="μᵢ − λ·(Σw)ᵢ"
        />
        <KV
          label="contrib to return"
          value={fmtSigned(trace.contribution_to_return, 6)}
          hint="αᵢ · wᵢ"
          tone={
            trace.contribution_to_return > 0
              ? "pos"
              : trace.contribution_to_return < 0
              ? "neg"
              : "neutral"
          }
        />
      </div>
    </>
  );
}

// ---------- page ----------

function Arrow() {
  return (
    <div className="flex justify-center">
      <span className="text-zinc-600 text-xl">↓</span>
    </div>
  );
}

export default function TracePage() {
  const [status, setStatus] = useState<Status | null>(null);
  const [snapshots, setSnapshots] = useState<Record<string, Snapshot>>({});
  const [connected, setConnected] = useState(false);
  const [selected, setSelected] = useState<string>("");

  const refreshStatus = useCallback(async () => {
    try {
      const r = await fetch("/api/modules", { cache: "no-store" });
      const j = await r.json();
      setStatus(j.status);
    } catch {}
  }, []);

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

  useEffect(() => {
    const es = new EventSource("/api/events");
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.onmessage = (m) => {
      try {
        const e = JSON.parse(m.data) as {
          kind?: string;
          module: ModuleName | "system";
          snapshotName?: string;
          snapshotData?: Record<string, unknown>;
          ts?: string;
          receivedAt: string;
        };
        if (e.kind === "snapshot" && e.snapshotName && e.module !== "system") {
          const key = `${e.module}:${e.snapshotName}`;
          setSnapshots((prev) => ({
            ...prev,
            [key]: {
              module: e.module as ModuleName,
              name: e.snapshotName!,
              ts: e.ts ?? e.receivedAt,
              receivedAt: e.receivedAt,
              data: e.snapshotData ?? {},
            },
          }));
        }
        if (e.module === "system") refreshStatus();
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

  // ---- assemble per-asset trace from the four snapshot payloads ----
  const dataTraceSnap = snapshots["data:data.trace"];
  const forecastTraceSnap = snapshots["forecast:forecast.trace"];
  const riskTraceSnap = snapshots["risk:risk.trace"];
  const optTraceSnap = snapshots["opt:opt.trace"];

  const assetOptions = useMemo(() => {
    const fromData = dataTraceSnap
      ? Object.keys(
          (dataTraceSnap.data.assets as Record<string, unknown>) ?? {},
        )
      : [];
    const fromOpt = optTraceSnap
      ? Object.keys(
          (optTraceSnap.data.assets as Record<string, unknown>) ?? {},
        )
      : [];
    return Array.from(new Set([...fromData, ...fromOpt])).sort();
  }, [dataTraceSnap, optTraceSnap]);

  // Auto-pick the first asset when snapshots first load.
  useEffect(() => {
    if (!selected && assetOptions.length > 0) {
      const aapl = assetOptions.find((a) => a.endsWith(":AAPL"));
      setSelected(aapl ?? assetOptions[0]);
    }
  }, [assetOptions, selected]);

  const pick = <T,>(snap: Snapshot | undefined, symbol: string): T | undefined => {
    if (!snap) return undefined;
    const assets = (snap.data.assets ?? {}) as Record<string, T>;
    return assets[symbol];
  };

  const dataTrace = pick<DataAssetTrace>(dataTraceSnap, selected);
  const forecastTrace = pick<ForecastAssetTrace>(forecastTraceSnap, selected);
  const riskTrace = pick<RiskAssetTrace>(riskTraceSnap, selected);
  const optTrace = pick<OptAssetTrace>(optTraceSnap, selected);
  const riskAversion = (optTraceSnap?.data.risk_aversion as number) ?? undefined;

  const seqLine = (() => {
    const parts = [
      dataTraceSnap && ["data", dataTraceSnap.data.seq],
      forecastTraceSnap && ["forecast", forecastTraceSnap.data.seq],
      riskTraceSnap && ["risk", riskTraceSnap.data.seq],
      optTraceSnap && ["opt", optTraceSnap.data.seq],
    ].filter(Boolean) as [string, unknown][];
    return parts.map(([m, s]) => `${m}#${s}`).join("  ");
  })();

  const noData = assetOptions.length === 0;
  const anyRunning = status
    ? Object.values(status).some((s) => s.running)
    : false;

  return (
    <div className="min-h-screen bg-black text-zinc-200 font-sans p-4">
      <NavBar
        status={status}
        connected={connected}
        onStartAll={() => act("start_all")}
        onStopAll={() => act("stop_all")}
      />

      <div className="flex flex-wrap items-center gap-3 mb-3">
        <label className="text-xs text-zinc-500">Asset</label>
        <select
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
          disabled={noData}
          className="bg-zinc-900 border border-zinc-700 rounded px-2 py-1 text-sm font-mono disabled:opacity-40"
        >
          {noData ? (
            <option>no trace yet — start the pipeline</option>
          ) : (
            assetOptions.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))
          )}
        </select>
        <span className="text-xs text-zinc-600 font-mono">{seqLine}</span>
        {!anyRunning && !noData && (
          <span className="text-xs text-amber-400">pipeline stopped</span>
        )}
      </div>

      {noData ? (
        <p className="text-sm text-zinc-500 p-4 border border-zinc-800 rounded">
          Start the pipeline from the Overview page, then come back here.
        </p>
      ) : (
        <div className="flex flex-col gap-2">
          <StepCard
            title="1. Raw Data"
            accent="border-sky-500"
            subtitle="OHLCV → log returns"
          >
            <DataTraceBody trace={dataTrace} />
          </StepCard>
          <Arrow />
          <StepCard
            title="2. Forecast"
            accent="border-emerald-500"
            subtitle="raw score → z-score → IR-weighted contribution → α"
          >
            <ForecastTraceBody trace={forecastTrace} />
          </StepCard>
          <Arrow />
          <StepCard
            title="3. Risk"
            accent="border-amber-500"
            subtitle="variance & correlations from the cov matrix"
          >
            <RiskTraceBody trace={riskTrace} />
          </StepCard>
          <Arrow />
          <StepCard
            title="4. Optimization (MVO)"
            accent="border-cyan-500"
            subtitle="(α, σ²) → weight via mean-variance utility"
          >
            <OptTraceBody trace={optTrace} riskAversion={riskAversion} />
          </StepCard>
        </div>
      )}

      <p className="text-[10px] text-zinc-600 mt-4">
        Each card is sourced from the matching{" "}
        <code>&lt;module&gt;.trace</code> snapshot. The{" "}
        <code>seq</code> row above shows which cycle each card belongs to —
        mismatched seqs indicate one of the modules is lagging.
      </p>
    </div>
  );
}
