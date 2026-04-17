/**
 * Headless-Chromium smoke test for the unified graph editor.
 *
 * Covers:
 *   - /graph loads and LiteGraph renders the persisted unified graph.
 *   - Palette add marks the editor dirty.
 *   - Save persists the modified graph to prototype/graph.json.
 *   - Reload reflects what is on disk.
 *
 * Restores prototype/graph.json to its original bytes at exit.
 */

import { chromium } from "playwright";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PORT = process.env.PORT ?? "3100";
const BASE = `http://127.0.0.1:${PORT}`;
const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, "..", "..", "..");
const GRAPH_PATH = path.join(REPO, "prototype", "graph.json");

function assert(cond, msg) {
  if (!cond) throw new Error("ASSERT: " + msg);
}

async function main() {
  const original = await readFile(GRAPH_PATH, "utf8");
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const consoleErrors = [];

  page.on("console", (msg) => {
    const type = msg.type();
    if (type === "error") consoleErrors.push(msg.text());
    if (type !== "debug") {
      console.log(`[browser ${type}] ${msg.text()}`);
    }
  });
  page.on("pageerror", (error) => {
    consoleErrors.push(String(error));
    console.log("[pageerror] " + error);
  });

  try {
    await page.goto(BASE + "/graph");
    await page.waitForSelector(".editor-root", { timeout: 10_000 });
    await page.waitForFunction(
      () => /11 nodes/.test(document.querySelector('[data-testid="editor-hud"]')?.textContent ?? ""),
      null,
      { timeout: 15_000 },
    );
    console.log("ok  initial render: unified graph · 11 nodes");

    const before = await page.locator('[data-testid="editor-hud"]').textContent();
    await page.locator('.editor-palette-item[data-type="data/Clock"]').click();
    await page.waitForFunction(
      () => /12 nodes/.test(document.querySelector('[data-testid="editor-hud"]')?.textContent ?? ""),
      null,
      { timeout: 5_000 },
    );
    console.log(`ok  palette add: ${before.trim()} → unified graph · 12 nodes`);

    const putPromise = page.waitForResponse(
      (response) => response.url().endsWith("/api/graph") && response.request().method() === "PUT",
      { timeout: 10_000 },
    );
    await page.locator("button:has-text('Save')").click();
    const putResponse = await putPromise;
    if (!putResponse.ok()) {
      throw new Error(`PUT failed: ${putResponse.status()} ${await putResponse.text()}`);
    }
    await page.waitForFunction(
      () => /saved graph/.test(document.querySelector('[role="status"]')?.textContent ?? ""),
      null,
      { timeout: 5_000 },
    );
    const afterSave = JSON.parse(await readFile(GRAPH_PATH, "utf8"));
    assert(afterSave.nodes.length === 12, `disk should have 12 nodes, got ${afterSave.nodes.length}`);
    console.log(`ok  save persisted ${afterSave.nodes.length} nodes to prototype/graph.json`);

    await page.locator("button:has-text('Reload')").click();
    await page.waitForFunction(
      () => /12 nodes/.test(document.querySelector('[data-testid="editor-hud"]')?.textContent ?? ""),
      null,
      { timeout: 5_000 },
    );
    console.log("ok  reload matches disk: 12 nodes");

    await page.screenshot({
      path: path.join(HERE, "screenshot.png"),
      fullPage: false,
    });
    console.log("ok  screenshot saved");

    if (consoleErrors.length) {
      throw new Error(`console had errors:\n${consoleErrors.join("\n")}`);
    }

    console.log("\nall smoke checks passed.");
  } finally {
    await browser.close();
    await writeFile(GRAPH_PATH, original, "utf8");
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
