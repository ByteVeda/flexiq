import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-bp-")), "q.db") });
}

async function waitFor(predicate: () => boolean, timeoutMs = 14000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("never runs more jobs at once than concurrency", async () => {
  // The dispatcher used to spawn every job it was handed and immediately take
  // the next, so the job channel drained as fast as the scheduler filled it and
  // nothing bounded in-flight work — the worker claimed jobs it could not run,
  // stranding them Running and starving peers on the same database.
  //
  // batchSize is the whole backlog on purpose. Several bounds overlap here —
  // poll cadence (~50ms), task duration, channel capacity, the in-flight cap —
  // and the test only proves something if the one under test is the tightest.
  // Claiming all 12 in a single poll removes cadence from the picture: unfixed,
  // all 12 dispatch at once and peak is 12; fixed, the in-flight clamp holds the
  // claim to 3 regardless of how fast the poller runs.
  const queue = newQueue();
  const total = 12;
  const concurrency = 3;
  let running = 0;
  let peak = 0;
  let done = 0;

  queue.task("slow", async () => {
    running += 1;
    peak = Math.max(peak, running);
    await new Promise((resolve) => setTimeout(resolve, 400));
    running -= 1;
    done += 1;
  });

  for (let i = 0; i < total; i += 1) {
    await queue.enqueue("slow");
  }

  worker = queue.runWorker({ queues: ["default"], concurrency, batchSize: total });

  expect(await waitFor(() => done >= total)).toBe(true);
  expect(peak).toBeLessThanOrEqual(concurrency);
  expect(peak).toBeGreaterThan(1);
});
