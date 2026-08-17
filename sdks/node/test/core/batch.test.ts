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
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-batch-")), "q.db") });
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

it("enqueueMany inserts a batch and returns ids in input order", async () => {
  const queue = newQueue();
  queue.task("double", (n: number) => n * 2);

  // Tag each row with a distinct queue so we can prove ids[i] maps to input[i].
  const ids = queue.enqueueMany("double", [
    { args: [1], options: { queue: "q0" } },
    { args: [2], options: { queue: "q1" } },
    { args: [3], options: { queue: "q2" } },
  ]);
  expect(ids).toHaveLength(3);
  expect(new Set(ids).size).toBe(3); // distinct ids
  expect((await queue.stats()).pending).toBe(3);

  // Returned ids must line up with the input rows, not just be present.
  expect(ids.map((id) => queue.getJob(id)?.queue)).toEqual(["q0", "q1", "q2"]);
});

it("runs every job in the batch", async () => {
  const queue = newQueue();
  const seen: number[] = [];
  queue.task("collect", (n: number) => {
    seen.push(n);
    return n;
  });

  const ids = queue.enqueueMany("collect", [{ args: [10] }, { args: [20] }, { args: [30] }]);
  worker = queue.runWorker();

  expect(await waitFor(() => seen.length === 3)).toBe(true);
  expect([...seen].sort((a, b) => a - b)).toEqual([10, 20, 30]);
  expect(await queue.result(ids[0] as string)).toBe(10);
});

it("applies per-job options across the batch", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const ids = queue.enqueueMany("noop", [
    { options: { queue: "a", priority: 5 } },
    { options: { queue: "b" } },
  ]);
  expect(queue.getJob(ids[0] as string)?.queue).toBe("a");
  expect(queue.getJob(ids[1] as string)?.queue).toBe("b");
});

it("dedups batch entries carrying a uniqueKey", async () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const active = queue.enqueue("noop", [], { uniqueKey: "k1" });

  // A key colliding with an active job, a fresh key repeated inside the batch,
  // and two keyless rows that always insert.
  const ids = queue.enqueueMany("noop", [
    { options: { uniqueKey: "k1" } },
    {},
    { options: { uniqueKey: "k2" } },
    { options: { uniqueKey: "k2" } },
    {},
  ]);

  expect(ids).toHaveLength(5); // one id per input row, in input order
  expect(ids[0]).toBe(active);
  expect(ids[2]).toBe(ids[3]);
  expect(new Set(ids).size).toBe(4); // only the two keyless rows are new
  expect((await queue.stats()).pending).toBe(4);
});

it("emits job.enqueued for deduped batch entries too", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  const enqueued: string[] = [];
  queue.on("job.enqueued", (event) => enqueued.push(event.jobId));

  const ids = queue.enqueueMany("noop", [
    { options: { uniqueKey: "dup" } },
    { options: { uniqueKey: "dup" } },
  ]);
  expect(enqueued).toEqual(ids); // one event per entry, matching enqueue()
});

it("runs onEnqueue interception for each batched job", () => {
  const queue = newQueue();
  let calls = 0;
  queue.use({
    onEnqueue: () => {
      calls += 1;
    },
  });
  queue.task("noop", () => undefined);

  queue.enqueueMany("noop", [{}, {}, {}]);
  expect(calls).toBe(3);
});
