// A registered worker reports a fingerprint of the tasks it can run.
//
// Discovery builds the registry at runtime, so a worker that registered part of
// it looks healthy and dead-letters every job for the rest. The fingerprint on
// the registry row is what makes that worker visible without going host by host.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";

// The value `crates/flexiq-core/BINDING_CONTRACT.md` pins for this task set.
// Hard-coded rather than recomputed here: a test that reimplemented the hash
// would agree with any drift in it, and the reason the constant matters is that
// a Node worker and a worker in another SDK have to produce the same string for
// the same registry.
const INVOICES_AND_REPORTS = "fafd30ef8ebcb7de";

let worker: Worker | undefined;
let queue: Queue | undefined;
let tempDir: string | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  await queue?.shutdown();
  queue = undefined;
  if (tempDir) {
    try {
      rmSync(tempDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    } catch {
      // the OS reclaims it
    }
    tempDir = undefined;
  }
});

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 20_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return false;
}

describe("worker registry fingerprint", () => {
  it("records a fingerprint of every registered handler", async () => {
    tempDir = mkdtempSync(join(tmpdir(), "flexiq-registry-"));
    const q = new Queue({ dbPath: join(tempDir, "q.db") });
    queue = q;
    // Registered in the opposite order to the fingerprint's, to pin that the
    // value is over the set rather than over registration order — which is
    // import order, and so decided by whatever discovered the tasks.
    q.task("reports.build", () => undefined);
    q.task("invoices.send", () => undefined);
    worker = q.runWorker({ concurrency: 1 });

    const registered = await waitFor(async () => (await q.listWorkers()).length > 0);
    expect(registered, "worker did not register").toBe(true);

    const [row] = await q.listWorkers();
    if (!row) {
      throw new Error("worker registered but listWorkers returned nothing");
    }

    expect(row.registryFingerprint).toBe(INVOICES_AND_REPORTS);
  });
});
