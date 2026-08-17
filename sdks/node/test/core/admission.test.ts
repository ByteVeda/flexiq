// S26 — opt-in `maxPending` admission cap. Jobs stay pending without a worker,
// so the cap is exercised purely producer-side.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue, QueueError, QueueFullError } from "../../src/index";

function newQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-adm-")), "queue.db");
  return new Queue({ dbPath });
}

it("countPendingByQueue counts pending per queue", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  expect(queue.countPendingByQueue("default")).toBe(0);
  queue.enqueue("noop");
  queue.enqueue("noop");
  expect(queue.countPendingByQueue("default")).toBe(2);
});

it("uncapped queue never rejects", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  for (let i = 0; i < 25; i++) queue.enqueue("noop");
  expect(queue.countPendingByQueue("default")).toBe(25);
});

it("rejects at the configured cap", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  queue.configureQueue("default", { maxPending: 2 });
  queue.enqueue("noop");
  queue.enqueue("noop");
  expect(() => queue.enqueue("noop")).toThrow(QueueFullError);
  // Rejected enqueue inserted nothing.
  expect(queue.countPendingByQueue("default")).toBe(2);
});

it("cap is per queue", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  queue.configureQueue("tight", { maxPending: 1 });
  queue.enqueue("noop", undefined, { queue: "tight" });
  expect(() => queue.enqueue("noop", undefined, { queue: "tight" })).toThrow(QueueFullError);
  for (let i = 0; i < 5; i++) queue.enqueue("noop", undefined, { queue: "wide" });
});

it("enqueueMany is all-or-nothing against the cap", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  queue.configureQueue("default", { maxPending: 3 });
  queue.enqueue("noop");
  queue.enqueue("noop");
  queue.enqueue("noop");
  expect(() => queue.enqueueMany("noop", [{}, {}, {}])).toThrow(QueueFullError);
  expect(queue.countPendingByQueue("default")).toBe(3);
});

it("enqueueMany accounts for the batch size", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  queue.configureQueue("default", { maxPending: 3 });
  // Empty queue, but a batch bigger than the cap is rejected as a whole.
  expect(() => queue.enqueueMany("noop", [{}, {}, {}, {}])).toThrow(QueueFullError);
  expect(queue.countPendingByQueue("default")).toBe(0);
  // A batch that exactly fits is admitted.
  queue.enqueueMany("noop", [{}, {}, {}]);
  expect(queue.countPendingByQueue("default")).toBe(3);
  // Now full: one more is rejected.
  expect(() => queue.enqueue("noop")).toThrow(QueueFullError);
});

it("configureQueue rejects a negative cap", () => {
  const queue = newQueue();
  expect(() => queue.configureQueue("default", { maxPending: -1 })).toThrow(RangeError);
});

it("QueueFullError is a QueueError", () => {
  const err = new QueueFullError("q", 5, 5);
  expect(err).toBeInstanceOf(QueueError);
  expect(err.queue).toBe("q");
  expect(err.cap).toBe(5);
});
