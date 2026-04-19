import "server-only";

import { readFile, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";

import { graphCliPath, graphPath, pythonExecutable, repoRoot, runtimePath } from "@/lib/paths";
import type { GraphDoc, NodeSchema, RunResult } from "@/lib/types";

let schemaCache: NodeSchema[] | null = null;
let inflightSchemas: Promise<NodeSchema[]> | null = null;

export async function readGraph(): Promise<GraphDoc> {
  const raw = await readFile(graphPath(), "utf8");
  return JSON.parse(raw) as GraphDoc;
}

export async function writeGraph(payload: unknown): Promise<GraphDoc> {
  const doc = normalizeGraph(payload);
  await validateGraphDoc(doc);
  await writeFile(
    graphPath(),
    JSON.stringify(doc, null, 2) + "\n",
    "utf8",
  );
  return doc;
}

export async function loadSchemas(refresh = false): Promise<NodeSchema[]> {
  if (refresh) schemaCache = null;
  if (schemaCache) return schemaCache;
  if (!inflightSchemas) {
    inflightSchemas = runPython([graphCliPath(), "schemas"])
      .then((raw) => JSON.parse(raw) as NodeSchema[])
      .finally(() => {
        inflightSchemas = null;
      });
  }
  schemaCache = await inflightSchemas;
  return schemaCache;
}

export async function runGraph(ticks = 1): Promise<RunResult> {
  return runPythonDetailed([
    runtimePath(),
    "--graph",
    graphPath(),
    "--ticks",
    String(Math.max(1, Math.trunc(ticks))),
  ]);
}

async function validateGraphDoc(doc: GraphDoc): Promise<void> {
  await runPython([graphCliPath(), "validate", "-"], JSON.stringify(doc));
}

function runPython(args: string[], stdin?: string): Promise<string> {
  return runPythonDetailed(args, stdin).then((result) => result.stdout);
}

function runPythonDetailed(args: string[], stdin?: string): Promise<RunResult> {
  return new Promise((resolve, reject) => {
    const proc = spawn(pythonExecutable(), args, {
      cwd: repoRoot(),
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env, PYTHONUNBUFFERED: "1" },
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    proc.stdout.on("data", (chunk) => stdout.push(chunk));
    proc.stderr.on("data", (chunk) => stderr.push(chunk));
    if (stdin) {
      proc.stdin.write(stdin, "utf8");
    }
    proc.stdin.end();
    proc.on("error", reject);
    proc.on("close", (code) => {
      const result: RunResult = {
        ok: code === 0,
        code: code ?? -1,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
      };
      if (code === 0) {
        resolve(result);
        return;
      }
      reject(
        new PythonCommandError(
          result.stderr || result.stdout || `exit ${code}`,
          result,
        ),
      );
    });
  });
}

function normalizeGraph(payload: unknown): GraphDoc {
  if (!payload || typeof payload !== "object") {
    throw new Error("payload must be a JSON object");
  }

  const doc = payload as Partial<GraphDoc>;
  if (!Array.isArray(doc.nodes) || !Array.isArray(doc.edges)) {
    throw new Error("payload must have nodes[] and edges[]");
  }

  return {
    nodes: doc.nodes.map((node) => {
      if (!node || typeof node !== "object") {
        throw new Error("each node must be an object");
      }
      if (typeof node.id !== "string" || !node.id) {
        throw new Error("node.id must be a non-empty string");
      }
      if (typeof node.type !== "string" || !node.type) {
        throw new Error("node.type must be a non-empty string");
      }
      return {
        id: node.id,
        type: node.type,
        params: { ...(node.params ?? {}) },
        pos: [Number(node.pos?.[0] ?? 0), Number(node.pos?.[1] ?? 0)],
        size: [Number(node.size?.[0] ?? 0), Number(node.size?.[1] ?? 0)],
      };
    }),
    edges: doc.edges.map((edge) => {
      if (!edge || typeof edge !== "object") {
        throw new Error("each edge must be an object");
      }
      for (const key of ["src_node", "src_port", "dst_node", "dst_port"] as const) {
        const value = edge[key];
        if (typeof value !== "string" || !value) {
          throw new Error(`edge missing string field: ${key}`);
        }
      }
      return {
        src_node: edge.src_node,
        src_port: edge.src_port,
        dst_node: edge.dst_node,
        dst_port: edge.dst_port,
      };
    }),
  };
}

export class PythonCommandError extends Error {
  readonly result: RunResult;

  constructor(message: string, result: RunResult) {
    super(message);
    this.name = "PythonCommandError";
    this.result = result;
  }
}
