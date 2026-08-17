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
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-tc-")), "q.db") });
}

/** Poll an async predicate. `waitFor` takes a sync one — a promise is always truthy there. */
async function waitForAsync(
  predicate: () => Promise<boolean>,
  timeoutMs = 10000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

async function waitFor(predicate: () => boolean, timeoutMs = 10000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("registers a task that sets only maxInFlightPerTask", async () => {
  // The config collector used to gate on a hand-listed set of option names, so
  // a task setting only an option missing from that list was dropped before it
  // reached the scheduler — silently, and invisibly to type-checking. A job
  // still runs either way, so assert the option reaches native by giving it a
  // value the scheduler must honour: one slot means strictly serial execution.
  const queue = newQueue();
  const total = 4;
  let running = 0;
  let peak = 0;
  let done = 0;

  queue.task(
    "solo",
    async () => {
      running += 1;
      peak = Math.max(peak, running);
      await new Promise((resolve) => setTimeout(resolve, 150));
      running -= 1;
      done += 1;
    },
    { maxInFlightPerTask: 1 },
  );

  for (let i = 0; i < total; i += 1) {
    await queue.enqueue("solo");
  }

  worker = queue.runWorker({ queues: ["default"], concurrency: 4, batchSize: total });

  expect(await waitFor(() => done >= total)).toBe(true);
  expect(peak).toBe(1);
});

it("rejects a malformed rateLimit rather than silently disabling it", async () => {
  const queue = newQueue();
  queue.task("bad", async () => {}, { rateLimit: "100/mm" as never });
  expect(() => queue.runWorker({ queues: ["default"] })).toThrow(/rateLimit/);
});

it("registers a task that sets only retryBudget", async () => {
  // Nothing was added to the config gate for this option — it derives from the
  // built config — so this asserts a task setting only retryBudget still reaches
  // the scheduler. One token means the second failure dead-letters rather than
  // retrying, even though maxRetries would allow more.
  const queue = newQueue();

  queue.task(
    "flaky",
    async () => {
      throw new Error("dependency down");
    },
    { retryBudget: "1/m", maxRetries: 5 },
  );

  for (let i = 0; i < 3; i += 1) {
    await queue.enqueue("flaky");
  }

  worker = queue.runWorker({ queues: ["default"], concurrency: 4, batchSize: 3 });

  const budgetKilled = async (): Promise<number> => {
    const dead = await queue.deadLetters(10);
    return dead.filter((entry) => entry.metadata === "retry_budget_exhausted").length;
  };
  expect(await waitForAsync(async () => (await budgetKilled()) > 0)).toBe(true);
});

it("rejects a malformed onExcess rather than silently deferring", async () => {
  const queue = newQueue();
  queue.task("bad", async () => {}, { onExcess: "discard" as never });
  expect(() => queue.runWorker({ queues: ["default"] })).toThrow(/onExcess/);
});

it("sheds rate-limited jobs to the dead-letter queue under onExcess: drop", async () => {
  // One token per hour: exactly one of the four jobs can dispatch, and the
  // other three must terminate instead of waiting out the limit. The limiter
  // gates at dispatch, so this asserts on dead-letter rows and pending depth,
  // never on when the handler ran.
  const queue = newQueue();
  const total = 4;

  queue.task("sample", async () => {}, { rateLimit: "1/h", onExcess: "drop" });

  for (let i = 0; i < total; i += 1) {
    await queue.enqueue("sample");
  }

  worker = queue.runWorker({ queues: ["default"], concurrency: 2, batchSize: total });

  const shed = async (): Promise<number> => {
    const dead = await queue.deadLetters(10);
    return dead.filter((entry) => entry.error?.startsWith("rate_limit:")).length;
  };
  expect(await waitForAsync(async () => (await shed()) === total - 1)).toBe(true);
  expect(await waitForAsync(async () => (await queue.stats()).completed === 1)).toBe(true);

  const stats = await queue.stats();
  expect(stats.pending).toBe(0);
  // Shedding loses no job silently: each one either ran or reached the DLQ.
  expect(stats.completed + stats.dead).toBe(total);
});
