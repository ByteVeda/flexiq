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

// #695 — the cap is admission control on pending rows, and a coalescing enqueue
// adds none, so a debounced enqueue carries the cap into the write instead of
// being checked against it producer-side.

function debouncing(): Queue {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  queue.task("report", (_userId: number) => undefined, {
    debounce: "5m",
    debounceKey: "report:{0}",
    debounceMaxWait: "30m",
  });
  return queue;
}

it("a full queue still takes a debounced slide", () => {
  const queue = debouncing();
  queue.configureQueue("default", { maxPending: 2 });
  const opened = queue.enqueue("report", [7]);
  queue.enqueue("noop");
  expect(queue.countPendingByQueue("default")).toBe(2);

  expect(queue.enqueue("report", [7])).toBe(opened);
  expect(queue.countPendingByQueue("default")).toBe(2);
});

it("a full queue still refuses to open a debounce window", () => {
  const queue = debouncing();
  queue.configureQueue("default", { maxPending: 2 });
  queue.enqueue("noop");
  queue.enqueue("noop");

  try {
    queue.enqueue("report", [7]);
    expect.unreachable("the cap must refuse an enqueue that opens a window");
  } catch (error) {
    expect(error).toBeInstanceOf(QueueFullError);
    expect((error as QueueFullError).queue).toBe("default");
    expect((error as QueueFullError).pending).toBe(2);
    expect((error as QueueFullError).cap).toBe(2);
  }
  expect(queue.countPendingByQueue("default")).toBe(2);
});
