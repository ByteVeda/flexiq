import { execSync } from "node:child_process";
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { resolveAutoscaleConfig, setLogLevel, WorkerProcessManager } from "../../src/index";

const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
const distIndex = fileURLToPath(new URL("../../dist/index.js", import.meta.url));
const indexUrl = pathToFileURL(distIndex).href;

beforeAll(() => {
  // Spawned workers import the package the way a user would, so they need dist.
  if (!existsSync(distIndex)) {
    execSync("pnpm run build:ts", { cwd: pkgRoot, stdio: "ignore" });
  }
}, 60_000);

let manager: WorkerProcessManager | undefined;

afterEach(() => {
  manager?.killAll();
  manager = undefined;
  setLogLevel("warn");
});

/** An app module that registers one task and enqueues a job for itself. */
function writeApp(): { app: string; marker: string } {
  const dir = mkdtempSync(join(tmpdir(), "flexiq-autoscale-"));
  const marker = join(dir, "done.txt");
  const app = join(dir, "app.mjs");
  // Static module body — paths arrive via env, never interpolated into source.
  writeFileSync(
    app,
    [
      `const { Queue } = await import(process.env.FLEXIQ_INDEX);`,
      `const { writeFileSync } = await import("node:fs");`,
      `const queue = new Queue({ dbPath: process.env.FLEXIQ_DB });`,
      `queue.task("mark", () => { writeFileSync(process.env.FLEXIQ_MARKER, "ok"); return "ok"; });`,
      `queue.enqueue("mark");`,
      `export default queue;`,
    ].join("\n"),
  );
  process.env.FLEXIQ_INDEX = indexUrl;
  process.env.FLEXIQ_DB = join(dir, "autoscale.db");
  process.env.FLEXIQ_MARKER = marker;
  return { app, marker };
}

function poolFor(app: string): WorkerProcessManager {
  manager = new WorkerProcessManager(
    resolveAutoscaleConfig({ app, concurrencyPerWorker: 2, drainTimeoutMs: 2_000 }),
  );
  return manager;
}

describe("WorkerProcessManager", () => {
  it("spawns a worker that runs the app's tasks, then drains it", async () => {
    const { app, marker } = writeApp();
    const pool = poolFor(app);

    const pid = pool.spawnWorker();
    expect(pool.livePids()).toEqual([pid]);
    expect(await waitFor(() => existsSync(marker), 10_000)).toBe(true);

    expect(await pool.terminateWorker(pid)).toBe(true);
    expect(pool.countLive()).toBe(0);
    // A drain we asked for is not a crash.
    expect(pool.reapDead()).toEqual([]);
  }, 20_000);

  it("reports a worker that died on its own as crashed", async () => {
    const { app } = writeApp();
    const pool = poolFor(app);

    const pid = pool.spawnWorker();
    // The crash warning is the point of the test; keep its line out of the run.
    setLogLevel("silent");
    process.kill(pid, "SIGKILL");

    expect(await waitFor(() => pool.countLive() === 0, 5_000)).toBe(true);
    expect(pool.reapDead()).toEqual([pid]);
    // Reaping clears the list — a crash is reported once.
    expect(pool.reapDead()).toEqual([]);
  }, 20_000);
});

async function waitFor(condition: () => boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return false;
}
