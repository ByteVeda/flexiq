import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(async () => {
  await worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-requeue-")), "q.db") });
}

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return false;
}

it("declines to requeue a job that is not running", () => {
  const queue = newQueue();
  queue.task("noop", () => "ok");
  expect(queue.requeueJob(queue.enqueue("noop"))).toBe(false);
});

it("declines to requeue an unknown job id", () => {
  expect(newQueue().requeueJob("no-such-job")).toBe(false);
});

it("returns a stuck running job to pending without spending retry budget", async () => {
  const queue = newQueue();
  let release: () => void = () => {};
  const hung = new Promise<void>((resolve) => {
    release = resolve;
  });
  queue.task("hangs", async () => {
    await hung;
  });

  const id = queue.enqueue("hangs", [], { maxRetries: 3 });
  worker = queue.runWorker();
  expect(await waitFor(() => queue.getJob(id)?.status === "running")).toBe(true);

  // Stop dispatch so the requeued job isn't immediately re-claimed; the handler
  // stays hung, which is exactly the stuck-Running state operators hit.
  await worker.stop();
  worker = undefined;

  expect(queue.requeueJob(id)).toBe(true);
  const job = queue.getJob(id);
  expect(job?.status).toBe("pending");
  expect(job?.retryCount).toBe(0);
  expect(job?.startedAt).toBeUndefined();

  // A second call finds it Pending, not Running.
  expect(queue.requeueJob(id)).toBe(false);
  release();
});

it("lets a healthy worker pick up the requeued job", async () => {
  const queue = newQueue();
  let release: () => void = () => {};
  const hung = new Promise<void>((resolve) => {
    release = resolve;
  });
  let runs = 0;
  queue.task("hangsOnce", async () => {
    runs += 1;
    if (runs === 1) {
      await hung;
    }
    return "done";
  });

  const id = queue.enqueue("hangsOnce");
  const stuck = queue.runWorker();
  expect(await waitFor(() => queue.getJob(id)?.status === "running")).toBe(true);
  await stuck.stop();

  // The claim released by requeueJob is what lets the second worker take it.
  expect(queue.requeueJob(id)).toBe(true);
  worker = queue.runWorker();
  expect(await waitFor(() => queue.getJob(id)?.status === "complete")).toBe(true);
  expect(runs).toBe(2);
  release();
});
