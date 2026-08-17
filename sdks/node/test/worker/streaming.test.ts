import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { currentJob, Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-stream-")), "q.db") });
}

it("streams partial results published by a task, in order", async () => {
  const queue = newQueue();
  queue.task("crunch", async () => {
    const job = currentJob();
    for (let i = 1; i <= 3; i += 1) {
      job?.publish({ step: i });
      await new Promise((r) => setTimeout(r, 15));
    }
    return "done";
  });

  const id = queue.enqueue("crunch");
  worker = queue.runWorker();

  const got: unknown[] = [];
  for await (const value of queue.stream(id, { pollMs: 10, timeoutMs: 5000 })) {
    got.push(value);
  }
  expect(got).toEqual([{ step: 1 }, { step: 2 }, { step: 3 }]);
  expect(await queue.result(id)).toBe("done");
});

it("drains partials already published before the consumer attaches", async () => {
  const queue = newQueue();
  queue.task("emit", () => {
    const job = currentJob();
    job?.publish("a");
    job?.publish("b");
    return null;
  });

  const id = queue.enqueue("emit");
  worker = queue.runWorker();
  await queue.result(id); // let the job finish first

  const got: unknown[] = [];
  for await (const value of queue.stream(id, { pollMs: 10, timeoutMs: 2000 })) {
    got.push(value);
  }
  expect(got).toEqual(["a", "b"]);
});

it("terminates an empty stream when the job has no partials", async () => {
  const queue = newQueue();
  queue.task("quiet", () => 1);

  const id = queue.enqueue("quiet");
  worker = queue.runWorker();

  const got: unknown[] = [];
  for await (const value of queue.stream(id, { pollMs: 10, timeoutMs: 2000 })) {
    got.push(value);
  }
  expect(got).toEqual([]);
});

it("exposes raw task logs", async () => {
  const queue = newQueue();
  queue.task("emit", () => {
    currentJob()?.publish({ x: 1 });
    return null;
  });

  const id = queue.enqueue("emit");
  worker = queue.runWorker();
  await queue.result(id);

  const logs = queue.taskLogs(id).filter((l) => l.level === "result");
  expect(logs).toHaveLength(1);
  expect(JSON.parse(logs[0]?.extra as string)).toEqual({ x: 1 });
});
