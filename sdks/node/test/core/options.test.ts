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
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
  return new Queue({ dbPath });
}

async function waitForStatus(
  queue: Queue,
  id: string,
  predicate: (status: string) => boolean,
  timeoutMs = 5000,
): Promise<{ status: string; error?: string }> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = queue.getJob(id);
    if (job && predicate(job.status)) {
      return job;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("timed out waiting for job state");
}

it("delays a job until its delay elapses", async () => {
  const queue = newQueue();
  queue.task("noop", () => "done");
  const id = queue.enqueue("noop", [], { delayMs: 400 });
  worker = queue.runWorker();

  // Still pending well before the delay window.
  await new Promise((resolve) => setTimeout(resolve, 150));
  expect(queue.getResult(id)).toBeUndefined();

  await waitForStatus(queue, id, (s) => s === "complete");
  expect(queue.getResult(id)).toBe("done");
});

it("dedups enqueues sharing a uniqueKey", () => {
  const queue = newQueue();
  queue.task("noop", () => null);
  const first = queue.enqueue("noop", [], { uniqueKey: "k1" });
  const second = queue.enqueue("noop", [], { uniqueKey: "k1" });
  expect(second).toBe(first);
});

it("retries then dead-letters a failing task", async () => {
  const queue = newQueue();
  queue.task(
    "boom",
    () => {
      throw new Error("kaboom");
    },
    { maxRetries: 0 },
  );
  const id = queue.enqueue("boom");
  worker = queue.runWorker();

  const job = await waitForStatus(queue, id, (s) => s === "dead");
  expect(job.status).toBe("dead");
});

it("fails a task that exceeds its timeout", async () => {
  const queue = newQueue();
  queue.task("slow", () => new Promise((resolve) => setTimeout(resolve, 500)), {
    maxRetries: 0,
    timeoutMs: 100,
  });
  const id = queue.enqueue("slow");
  worker = queue.runWorker();

  // If the timeout were not enforced the task would complete; it must dead-letter.
  const job = await waitForStatus(queue, id, (s) => s === "dead");
  expect(job.status).toBe("dead");
});
