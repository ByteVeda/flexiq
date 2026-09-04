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
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-rb-")), "q.db") });
}

async function waitFor(predicate: () => boolean, timeoutMs = 20_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("reports every job exactly once when results are drained as a batch", async () => {
  // The drain loop finalizes everything already queued in one transaction, so a
  // burst of results is handled as a batch rather than one per wake. Successes
  // batch into a single write while failures stay on the per-result path, so a
  // mixed burst exercises both halves: each job must still surface exactly one
  // outcome, none lost and none double-counted.
  const queue = newQueue();
  const each = 6;
  const completed: string[] = [];
  const dead: string[] = [];

  queue.on("job.completed", (event) => completed.push(event.jobId));
  queue.on("job.dead", (event) => dead.push(event.jobId));

  queue.task("ok", (n: number) => n);
  queue.task(
    "boom",
    () => {
      throw new Error("expected");
    },
    { maxRetries: 0 },
  );

  for (let i = 0; i < each; i += 1) {
    await queue.enqueue("ok", [i]);
    await queue.enqueue("boom");
  }

  worker = queue.runWorker({ queues: ["default"], concurrency: 8, batchSize: each * 2 });

  expect(await waitFor(() => completed.length >= each && dead.length >= each)).toBe(true);

  expect(completed).toHaveLength(each);
  expect(dead).toHaveLength(each);
  expect(new Set(completed).size).toBe(each);
  expect(new Set(dead).size).toBe(each);
  // Uniqueness within each list would still miss a job reported once as both.
  const overlap = completed.filter((id) => dead.includes(id));
  expect(overlap).toEqual([]);
});
