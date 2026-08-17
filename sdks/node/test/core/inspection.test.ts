import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { type DeadJob, JobFailedError, Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
  return new Queue({ dbPath });
}

it("reports stats and lists jobs", async () => {
  const queue = newQueue();
  queue.task("add", (a: number, b: number) => a + b);
  const id = queue.enqueue("add", [1, 2]);

  expect((await queue.stats()).pending).toBe(1);
  expect((await queue.listJobs({ status: "pending" })).map((job) => job.id)).toContain(id);

  worker = queue.runWorker();
  expect(await queue.result(id)).toBe(3);
  expect((await queue.stats()).completed).toBe(1);
});

it("result() rejects with JobFailedError on a dead job", async () => {
  const queue = newQueue();
  queue.task(
    "boom",
    () => {
      throw new Error("nope");
    },
    { maxRetries: 0 },
  );
  const id = queue.enqueue("boom");
  worker = queue.runWorker();
  await expect(queue.result(id)).rejects.toBeInstanceOf(JobFailedError);
});

it("dead-letters a failing job and can retry it", async () => {
  const queue = newQueue();
  queue.task(
    "boom",
    () => {
      throw new Error("nope");
    },
    { maxRetries: 0 },
  );
  queue.enqueue("boom");
  worker = queue.runWorker();

  const dead = await waitForDead(queue);
  expect(dead.length).toBeGreaterThan(0);
  const entry = dead[0];
  if (!entry) {
    throw new Error("expected a dead-letter entry");
  }
  expect(typeof queue.retryDead(entry.id)).toBe("string");
});

it("lists and purges dead-letter entries by task", async () => {
  const queue = newQueue();
  const boom = () => {
    throw new Error("nope");
  };
  queue.task("alpha", boom, { maxRetries: 0 });
  queue.task("beta", boom, { maxRetries: 0 });
  queue.enqueue("alpha");
  queue.enqueue("alpha");
  queue.enqueue("beta");
  worker = queue.runWorker();

  const deadline = Date.now() + 10000;
  while (Date.now() < deadline && (await queue.deadLetters()).length < 3) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  await expect(queue.deadLettersByTask("alpha")).resolves.toHaveLength(2);
  await expect(queue.deadLettersByTask("beta")).resolves.toHaveLength(1);
  // Pagination applies within the task's own entries.
  await expect(queue.deadLettersByTask("alpha", 1, 1)).resolves.toHaveLength(1);

  await expect(queue.purgeDeadByTask("alpha")).resolves.toBe(2);
  await expect(queue.deadLettersByTask("alpha")).resolves.toHaveLength(0);
  await expect(queue.deadLetters()).resolves.toHaveLength(1);
});

it("pauses and resumes a queue", () => {
  const queue = newQueue();
  expect(queue.listPausedQueues()).not.toContain("default");
  queue.pauseQueue("default");
  expect(queue.listPausedQueues()).toContain("default");
  queue.resumeQueue("default");
  expect(queue.listPausedQueues()).not.toContain("default");
});

async function waitForDead(queue: Queue, timeoutMs = 10000): Promise<DeadJob[]> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const dead = await queue.deadLetters();
    if (dead.length > 0) {
      return dead;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return [];
}
