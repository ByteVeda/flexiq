import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { type EnqueuedEvent, Queue } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-qev-")), "q.db") });
}

it("emits queue.paused and queue.resumed", () => {
  const queue = newQueue();
  const seen: Array<{ event: string; queue: string }> = [];
  queue.on("queue.paused", (event) => seen.push({ event: "paused", queue: event.queue }));
  queue.on("queue.resumed", (event) => seen.push({ event: "resumed", queue: event.queue }));

  queue.pauseQueue("emails");
  expect(queue.listPausedQueues()).toContain("emails");
  queue.resumeQueue("emails");

  expect(seen).toEqual([
    { event: "paused", queue: "emails" },
    { event: "resumed", queue: "emails" },
  ]);
});

it("emits one job.enqueued per batch entry, in input order", () => {
  const queue = newQueue().task("add", (a: number, b: number) => a + b);
  const enqueued: EnqueuedEvent[] = [];
  queue.on("job.enqueued", (event) => enqueued.push(event));

  const jobIds = queue.enqueueMany("add", [
    { args: [1, 2] },
    { args: [3, 4], options: { queue: "emails" } },
    { args: [5, 6] },
  ]);

  expect(enqueued.map((event) => event.jobId)).toEqual(jobIds);
  expect(enqueued.map((event) => event.queue)).toEqual(["default", "emails", "default"]);
  expect(enqueued.every((event) => event.taskName === "add")).toBe(true);
});
