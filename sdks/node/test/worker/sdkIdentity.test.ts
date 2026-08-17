// A registered worker reports which SDK and release it runs.
//
// In a polyglot deployment the registry is the only place an operator can tell
// a stale worker from a current one without going host by host.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import manifest from "../../package.json" with { type: "json" };
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;
let queue: Queue | undefined;
let tempDir: string | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  // Shut down before removing the directory: the queue holds the SQLite file
  // open, and on Windows an open handle blocks the delete.
  await queue?.shutdown();
  queue = undefined;
  if (tempDir) {
    // Best effort. Shutdown stops dispatch but does not close storage — the
    // native handle goes away when the queue object is finalized, which JS
    // cannot force, so on Windows the unlink can still hit EBUSY. A temp dir
    // the runner reclaims anyway must not fail the test.
    try {
      rmSync(tempDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    } catch {
      // the OS reclaims it
    }
    tempDir = undefined;
  }
});

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 8000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return false;
}

describe("worker SDK identity", () => {
  it("records the SDK and its version on registration", async () => {
    // Bound locally as well as on the module-scoped handle the teardown uses,
    // so the closure below needs no non-null assertion.
    tempDir = mkdtempSync(join(tmpdir(), "flexiq-sdk-"));
    const q = new Queue({ dbPath: join(tempDir, "q.db") });
    queue = q;
    q.task("noop", () => undefined);
    worker = q.runWorker({ concurrency: 1 });

    const registered = await waitFor(async () => (await q.listWorkers()).length > 0);
    expect(registered, "worker did not register").toBe(true);

    const [row] = await q.listWorkers();
    if (!row) {
      throw new Error("worker registered but listWorkers returned nothing");
    }

    expect(row.sdk).toBe("node");
    // Compared against the package version rather than a literal, so the
    // assertion survives a version bump.
    expect(row.sdkVersion).toBe(manifest.version);
  });
});
