import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { mockResource, Queue, useResource, type Worker } from "../../src/index";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-rmetrics-")), "q.db") });
}

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((r) => setTimeout(r, 20));
  }
  return false;
}

it("counts a worker-scoped resource as created once, active until stop", async () => {
  const queue = newQueue();
  queue.resource("db", () => ({ ok: true }), { dispose: () => undefined });
  let seen = 0;
  queue.task("use", async () => {
    await useResource("db");
    seen += 1;
  });

  queue.enqueue("use");
  queue.enqueue("use");
  worker = queue.runWorker();

  expect(await waitFor(() => seen === 2)).toBe(true);
  let m = queue.resourceMetrics().db;
  expect(m).toEqual({ created: 1, disposed: 0, active: 1 }); // singleton, still alive

  worker.stop();
  worker = undefined;
  expect(await waitFor(() => queue.resourceMetrics().db?.disposed === 1)).toBe(true);
  m = queue.resourceMetrics().db;
  expect(m).toEqual({ created: 1, disposed: 1, active: 0 });
});

it("counts a task-scoped resource per job, all disposed", async () => {
  const queue = newQueue();
  queue.resource("tx", () => ({}), { scope: "task", dispose: () => undefined });
  queue.task("use", async () => {
    await useResource("tx");
  });

  queue.enqueue("use");
  queue.enqueue("use");
  worker = queue.runWorker();

  expect(await waitFor(() => queue.resourceMetrics().tx?.disposed === 2)).toBe(true);
  expect(queue.resourceMetrics().tx).toEqual({ created: 2, disposed: 2, active: 0 });
});

it("mockResource records resolutions and injects its value", async () => {
  const queue = newQueue();
  const db = mockResource({ query: () => 7 });
  queue.resource("db", db.factory);
  let result = 0;
  queue.task("read", async () => {
    const conn = await useResource<{ query: () => number }>("db");
    result = conn.query();
  });

  queue.enqueue("read");
  queue.enqueue("read");
  worker = queue.runWorker();

  expect(await waitFor(() => result === 7)).toBe(true);
  expect(db.resolutions).toBe(1); // worker-scoped singleton, built once
});
