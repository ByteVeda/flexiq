import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { currentJob, Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

async function waitFor(predicate: () => boolean, timeoutMs = 20_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return false;
}

it("cancels a running task cooperatively via the abort signal", async () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
  const queue = new Queue({ dbPath });

  let observedAbort = false;
  queue.task("longRunning", async () => {
    const signal = currentJob()?.signal;
    await new Promise<void>((resolve) => {
      if (!signal) {
        resolve();
        return;
      }
      signal.addEventListener("abort", () => {
        observedAbort = true;
        resolve();
      });
      // Safety net so a broken cancel path can't hang the suite.
      setTimeout(resolve, 3000);
    });
    // Reject so the dispatcher records a cancellation (not a success).
    throw new Error("aborted");
  });

  const id = queue.enqueue("longRunning");
  worker = queue.runWorker();

  expect(await waitFor(() => queue.getJob(id)?.status === "running")).toBe(true);
  expect(queue.requestCancel(id)).toBe(true);

  expect(await waitFor(() => queue.getJob(id)?.status === "cancelled")).toBe(true);
  expect(observedAbort).toBe(true);
});
