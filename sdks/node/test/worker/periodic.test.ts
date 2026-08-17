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

function freshQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-cron-")), "queue.db");
  return new Queue({ dbPath });
}

// The Rust `cron` crate uses 6/7-field expressions (seconds first):
// `sec min hour day-of-month month day-of-week [year]`.
it("registers a cron task and returns its next fire time", () => {
  const queue = freshQueue();
  queue.task("beat", () => null);

  const now = Date.now();
  const next = queue.registerPeriodic("heartbeat", "beat", "0 */5 * * * *", { args: [] });
  expect(next).toBeGreaterThan(now);

  // Re-register (upsert) the same name with a different schedule.
  const next2 = queue.registerPeriodic("heartbeat", "beat", "0 0 * * * *");
  expect(next2).toBeGreaterThan(now);
});

it("rejects an invalid cron expression", () => {
  const queue = freshQueue();
  queue.task("beat", () => null);
  expect(() => queue.registerPeriodic("bad", "beat", "not a cron")).toThrow();
});

it("lists, pauses, resumes, and deletes periodic tasks", () => {
  const queue = freshQueue();
  queue.task("beat", () => null);
  queue.registerPeriodic("nightly", "beat", "0 0 0 * * *");
  queue.registerPeriodic("hourly", "beat", "0 0 * * * *");

  expect(
    queue
      .listPeriodic()
      .map((p) => p.name)
      .sort(),
  ).toEqual(["hourly", "nightly"]);
  expect(queue.listPeriodic().find((p) => p.name === "nightly")?.enabled).toBe(true);

  // Pause toggles `enabled` but keeps the task in the catalog.
  expect(queue.pausePeriodic("nightly")).toBe(true);
  expect(queue.listPeriodic().find((p) => p.name === "nightly")?.enabled).toBe(false);
  expect(queue.listPeriodic()).toHaveLength(2);

  expect(queue.resumePeriodic("nightly")).toBe(true);
  expect(queue.listPeriodic().find((p) => p.name === "nightly")?.enabled).toBe(true);

  // Unknown name → not found.
  expect(queue.pausePeriodic("ghost")).toBe(false);

  // Delete removes it; a second delete reports not-found.
  expect(queue.deletePeriodic("nightly")).toBe(true);
  expect(queue.listPeriodic()).toHaveLength(1);
  expect(queue.deletePeriodic("nightly")).toBe(false);
});

it("accepts a circuit-breaker config and still runs the task", async () => {
  const queue = freshQueue();
  queue.task("guarded", (x: number) => x * 2, {
    circuitBreaker: { threshold: 3, windowMs: 60_000, cooldownMs: 60_000 },
  });
  const id = queue.enqueue("guarded", [21]);
  worker = queue.runWorker();
  expect(await queue.result(id, { timeoutMs: 8000 })).toBe(42);
});
