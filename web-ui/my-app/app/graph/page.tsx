"use client";

import dynamic from "next/dynamic";

import "./editor.css";

// LiteGraph touches window/document at module eval, so keep it out of SSR.
const GraphEditor = dynamic(() => import("@/components/GraphEditor"), {
  ssr: false,
  loading: () => <div style={{ padding: 20 }}>Loading editor…</div>,
});

export default function GraphPage() {
  return <GraphEditor />;
}
