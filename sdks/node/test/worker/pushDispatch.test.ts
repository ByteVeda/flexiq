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

// `pushDispatch` swaps the scheduler's poll loop for enqueue-driven wakeups when
// the addon carries the `push-dispatch` cargo feature, and is accepted and
// ignored (polling kept) when it doesn't. Either way a job must run to
// completion, so this covers both builds of the addon.
it("processes jobs with push dispatch requested", async () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-push-")), "queue.db");
  const queue = new Queue({ dbPath });
  queue.task("add", (a: number, b: number) => a + b);

  worker = queue.runWorker({ queues: ["default"], pushDispatch: true });
  const id = queue.enqueue("add", [2, 3]);

  expect(await queue.result(id, { timeoutMs: 8000 })).toBe(5);
});

// A job enqueued before the worker exists has no wake to ride on. The fallback
// timer (push builds) and the poll loop (default builds) must both still pick it
// up, so an early enqueue can never strand.
it("still dispatches work enqueued before the worker started", async () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-push-pre-")), "queue.db");
  const queue = new Queue({ dbPath });
  queue.task("echo", (value: string) => value);

  const id = queue.enqueue("echo", ["before-start"]);
  worker = queue.runWorker({ queues: ["default"], pushDispatch: true });

  expect(await queue.result(id, { timeoutMs: 8000 })).toBe("before-start");
});
