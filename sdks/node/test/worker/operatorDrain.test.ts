import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";

const running: Worker[] = [];

afterEach(async () => {
  await Promise.all(running.splice(0).map((worker) => worker.stop()));
});

function newQueue(dbPath?: string, namespace?: string): Queue {
  return new Queue({
    dbPath: dbPath ?? join(mkdtempSync(join(tmpdir(), "flexiq-drain-")), "q.db"),
    namespace,
  });
}

async function waitFor(predicate: () => Promise<boolean> | boolean, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

/** Start a worker with a fast heartbeat and wait for its row. */
async function startWorker(queue: Queue, queues?: string[]): Promise<string> {
  const before = new Set((await queue.listWorkers()).map((row) => row.workerId));
  let workerId = "";
  queue.on("worker.started", (event) => {
    if (!before.has(event.workerId) && workerId === "") {
      workerId = event.workerId;
    }
  });
  running.push(queue.runWorker({ queues, heartbeatIntervalMs: 50 }));
  expect(
    await waitFor(async () => (await queue.listWorkers()).some((row) => row.workerId === workerId)),
  ).toBe(true);
  return workerId;
}

describe("an operator's drain request", () => {
  it("stops the worker it names and unregisters it", async () => {
    const queue = newQueue();
    const stopped: string[] = [];
    queue.on("worker.stopped", (event) => stopped.push(event.workerId));
    queue.task("noop", () => undefined);
    const workerId = await startWorker(queue);

    await expect(queue.drainWorker(workerId)).resolves.toBe(true);

    expect(await waitFor(() => stopped.includes(workerId))).toBe(true);
    expect(await waitFor(async () => (await queue.listWorkers()).length === 0)).toBe(true);
    // Dispatch really stopped: a job enqueued after the drain stays pending.
    const id = queue.enqueue("noop");
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(queue.getJob(id)?.status).toBe("pending");
  });

  it("leaves a sibling worker on the same queue running", async () => {
    const queue = newQueue();
    const stopped: string[] = [];
    queue.on("worker.stopped", (event) => stopped.push(event.workerId));
    queue.task("noop", () => undefined);
    const drained = await startWorker(queue, ["a"]);
    const sibling = await startWorker(queue, ["b"]);

    await expect(queue.drainWorker(drained)).resolves.toBe(true);

    expect(await waitFor(() => stopped.includes(drained))).toBe(true);
    // `worker.stopped` fires before the row is unregistered; wait for that too.
    expect(
      await waitFor(async () =>
        (await queue.listWorkers()).every((row) => row.workerId !== drained),
      ),
    ).toBe(true);
    const rows = await queue.listWorkers();
    expect(rows.map((row) => row.workerId)).toEqual([sibling]);
    expect(rows[0]?.status).toBe("active");
    expect(stopped).not.toContain(sibling);
  });

  it("reaches only this namespace's workers", async () => {
    const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-drain-ns-")), "q.db");
    const mine = newQueue(dbPath, "mine");
    const theirs = newQueue(dbPath, "theirs");
    theirs.task("noop", () => undefined);
    const workerId = await startWorker(theirs);

    await expect(mine.drainWorker(workerId)).resolves.toBe(false);
    await expect(mine.drainWorker("no-such-worker")).resolves.toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 300));
    const rows = await theirs.listWorkers();
    expect(rows.map((row) => [row.workerId, row.status])).toEqual([[workerId, "active"]]);
  });
});
