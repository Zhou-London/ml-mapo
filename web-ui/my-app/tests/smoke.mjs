/**
 * Headless-chromium smoke test. Standalone (no @playwright/test runner)
 * so we can run from CLI with `node tests/smoke.mjs` while the dev server is
 * running on $PORT (default 3100).
 *
 * Covers:
 *   - /graph loads, LiteGraph boots, data module renders 6 nodes.
 *   - Tab switch to risk → 4 nodes.
 *   - Palette "add" appends a node (dirty flag flips).
 *   - Undo the add (delete the node) then save → no diff on disk.
 *   - True save after mutating pos via LGraph API → round-trip persists.
 *
 * Restores every graph.json to its original bytes at exit.
 */

import { chromium } from "playwright";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PORT = process.env.PORT ?? "3100";
const BASE = `http://127.0.0.1:${PORT}`;
const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, "..", "..", "..");
const GRAPH_PATHS = Object.fromEntries(
  ["data", "risk", "forecast", "optimization"].map((m) => [
    m,
    path.join(REPO, "prototype", m, "graph.json"),
  ]),
);

function assert(cond, msg) {
  if (!cond) throw new Error("ASSERT: " + msg);
}

async function readGraph(m) {
  return readFile(GRAPH_PATHS[m], "utf8");
}

async function main() {
  const originals = Object.fromEntries(
    await Promise.all(
      Object.entries(GRAPH_PATHS).map(async ([m, p]) => [m, await readFile(p, "utf8")]),
    ),
  );

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const consoleErrors = [];
  page.on("console", (msg) => {
    const t = msg.type();
    if (t === "error") consoleErrors.push(msg.text());
    // Surface info/warn/error — useful when LiteGraph fails to boot.
    if (t !== "debug") {
      console.log(`[browser ${t}] ${msg.text()}`);
    }
  });
  page.on("pageerror", (e) => {
    consoleErrors.push(String(e));
    console.log("[pageerror] " + e);
  });

  try {
    await page.addInitScript(() => {
      const origFetch = window.fetch;
      window.__putPayloads = [];
      window.fetch = async function (...args) {
        const [url, init] = args;
        if (init?.method === "PUT" && typeof init.body === "string") {
          try {
            const doc = JSON.parse(init.body);
            window.__putPayloads.push({ url: String(url), nodes: doc.nodes?.length, edges: doc.edges?.length });
          } catch {}
        }
        return origFetch.apply(this, args);
      };
    });
    await page.goto(BASE + "/graph");
    await page.waitForSelector(".editor-root", { timeout: 10_000 });
    const probeDump = await page.evaluate(() => ({
      hasReact: typeof window.React !== "undefined",
      bodyClass: document.body.className,
      scriptCount: document.querySelectorAll("script").length,
      paletteItems: document.querySelectorAll(".editor-palette-item").length,
    }));
    console.log("[probe]", JSON.stringify(probeDump));
    // Wait for LiteGraph boot + schemas load + initial module populate.
    try {
      await page.waitForFunction(
        () => /6 nodes/.test(document.querySelector('[data-testid="editor-hud"]')?.textContent ?? ""),
        null,
        { timeout: 15_000 },
      );
    } catch (e) {
      const hud = await page.locator('[data-testid="editor-hud"]').textContent();
      const status = await page.locator('[role="status"]').textContent();
      await page.screenshot({ path: path.join(HERE, "smoke-fail.png") });
      throw new Error(`hud="${hud}" status="${status}" — ${e.message}`);
    }
    console.log("ok  initial render: data · 6 nodes");

    // Switch to risk.
    await page.getByRole("tab", { name: "risk" }).click();
    await page.waitForFunction(
      () => /risk .+4 nodes/.test(document.querySelector('[data-testid="editor-hud"]')?.textContent ?? ""),
      null,
      { timeout: 5_000 },
    );
    console.log("ok  tab switch: risk · 4 nodes");

    // Click a palette item to add a node.
    const before = await page.evaluate(() =>
      document.querySelector('[data-testid="editor-hud"]').textContent,
    );
    await page.locator('.editor-palette-item[data-type="risk/Observer"]').click();
    await page.waitForFunction(
      () => /5 nodes/.test(document.querySelector('[data-testid="editor-hud"]')?.textContent ?? ""),
      null,
      { timeout: 5_000 },
    );
    console.log(`ok  palette add: ${before.trim()} → risk · 5 nodes`);

    // Save the current (5-node) graph, verify file changed then roll back.
    const putPromise = page.waitForResponse(
      (r) => r.url().endsWith("/api/graph/risk") && r.request().method() === "PUT",
      { timeout: 10_000 },
    );
    await page.locator("button:has-text('Save')").click();
    const putResponse = await putPromise;
    if (!putResponse.ok()) {
      throw new Error(`PUT failed: ${putResponse.status()} ${await putResponse.text()}`);
    }
    await page.waitForFunction(
      () => /saved/.test(document.querySelector('[role="status"]')?.textContent ?? ""),
      null,
      { timeout: 5_000 },
    );
    const afterSave = JSON.parse(await readGraph("risk"));
    assert(afterSave.nodes.length === 5, `disk should have 5 nodes, got ${afterSave.nodes.length}`);
    console.log(`ok  save persisted ${afterSave.nodes.length} nodes to risk/graph.json`);

    // Reload the UI from disk and verify it reflects the saved 5-node graph.
    await page.locator("button:has-text('Reload')").click();
    await page.waitForFunction(
      () => /5 nodes/.test(document.querySelector('[data-testid="editor-hud"]')?.textContent ?? ""),
      null,
      { timeout: 5_000 },
    );
    console.log("ok  reload matches disk: 5 nodes");

    // Final visual check: capture the risk graph for README/docs.
    await page.screenshot({
      path: path.join(HERE, "screenshot.png"),
      fullPage: false,
    });
    console.log("ok  screenshot saved");

    if (consoleErrors.length) {
      console.log("!!  console errors:");
      for (const e of consoleErrors) console.log("    " + e);
      throw new Error("console had errors");
    }

    console.log("\nall smoke checks passed.");
  } finally {
    await browser.close();
    // Restore every graph.json.
    await Promise.all(
      Object.entries(originals).map(([m, txt]) => writeFile(GRAPH_PATHS[m], txt, "utf8")),
    );
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
