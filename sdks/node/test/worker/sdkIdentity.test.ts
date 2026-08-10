// A registered worker reports which SDK and release it runs.
//
// In a polyglot deployment the registry is the only place an operator can tell
// a stale worker from a current one without going host by host.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import manifest from "../../package.json" with { type: "json" };
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;
let queue: Queue | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  await queue?.shutdown();
  queue = undefined;
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
    const q = new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "taskito-sdk-")), "q.db") });
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
