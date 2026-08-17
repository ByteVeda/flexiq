// S25 — opt-in LIFO dispatch order. Jobs are enqueued before the worker starts,
// so all pile up pending; a concurrency-1 worker then dispatches them one at a
// time and we record the order. UUIDv7 ids make same-millisecond ties
// deterministic, so LIFO is exactly newest-first.

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
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-order-")), "queue.db");
  return new Queue({ dbPath });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function runAndRecord(queue: Queue, total: number): Promise<number[]> {
  const order: number[] = [];
  queue.task("rec", (n: number) => {
    order.push(n);
  });
  for (let i = 0; i < total; i++) queue.enqueue("rec", [i]);

  worker = queue.runWorker({ concurrency: 1, batchSize: 1 });
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline && order.length < total) {
    await sleep(50);
  }
  return order;
}

it("LIFO dispatches newest-first", async () => {
  const queue = newQueue();
  queue.configureQueue("default", { dispatchOrder: "lifo" });
  const order = await runAndRecord(queue, 6);
  expect(order).toEqual([5, 4, 3, 2, 1, 0]);
});

it("FIFO is the default", async () => {
  const queue = newQueue();
  const order = await runAndRecord(queue, 6);
  expect(order).toEqual([0, 1, 2, 3, 4, 5]);
});
