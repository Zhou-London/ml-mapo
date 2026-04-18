import { existsSync } from "node:fs";
import path from "node:path";

// The web-ui lives two levels deep under the repo root:
//   <repo>/web-ui/my-app/lib/paths.ts
const REPO_ROOT = path.resolve(process.cwd(), "../..");
const PROTOTYPE_DIR = path.join(REPO_ROOT, "prototype");

export function pythonExecutable(): string {
  const venvPy = path.join(REPO_ROOT, ".venv", "bin", "python");
  if (existsSync(venvPy)) return venvPy;
  return process.env.PYTHON || "python3";
}

export function graphCliPath(): string {
  return path.join(PROTOTYPE_DIR, "graph_cli.py");
}

export function graphPath(): string {
  return path.join(PROTOTYPE_DIR, "graph.json");
}

export function runtimePath(): string {
  return path.join(PROTOTYPE_DIR, "main.py");
}

export function repoRoot(): string {
  return REPO_ROOT;
}
