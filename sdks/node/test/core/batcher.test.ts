import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { Batcher, Queue, QueueError } from "../../src/index";

function newQueue() {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "taskito-batcher-")), "q.db") }).task(
    "collect",
    (n: number) => n,
  );
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 4000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((r) => setTimeout(r, 20));
  }
  return false;
}

it("flushes once the buffer reaches maxSize", async () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 3, maxWaitMs: 60_000 });

  expect(batcher.add([1])).toEqual([]);
  expect(batcher.add([2])).toEqual([]);
  expect(batcher.size).toBe(2);
  expect((await queue.stats()).pending).toBe(0); // nothing enqueued yet

  const ids = batcher.add([3]);
  expect(ids).toHaveLength(3);
  expect(batcher.size).toBe(0);
  expect((await queue.stats()).pending).toBe(3);
});

it("flushes after maxWaitMs without reaching maxSize", async () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 100, maxWaitMs: 50 });

  batcher.add([1]);
  batcher.add([2]);
  expect(await waitFor(async () => (await queue.stats()).pending === 2)).toBe(true);
  expect(batcher.size).toBe(0);
  batcher.close();
});

it("close flushes the remainder and blocks further adds", async () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 100, maxWaitMs: 60_000 });

  batcher.add([1]);
  batcher.add([2]);
  expect(batcher.close()).toHaveLength(2);
  expect((await queue.stats()).pending).toBe(2);
  expect(batcher.closed).toBe(true);
  expect(() => batcher.add([3])).toThrow(QueueError);

  expect(batcher.close()).toEqual([]); // idempotent
});

it("flushes via Symbol.dispose", async () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 100, maxWaitMs: 60_000 });
  batcher.add([1]);
  expect((await queue.stats()).pending).toBe(0);

  batcher[Symbol.dispose]();
  expect((await queue.stats()).pending).toBe(1);
  expect(batcher.closed).toBe(true);
});

it("passes per-entry args and options through to enqueueMany", () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 2, maxWaitMs: 60_000 });

  batcher.add([1], { queue: "q0" });
  const ids = batcher.add([2], { queue: "q1" });
  expect(ids.map((id) => queue.getJob(id)?.queue)).toEqual(["q0", "q1"]);
});

it("batched jobs run like any other", async () => {
  const seen: number[] = [];
  const queue = newQueue().task("record", (n: number) => {
    seen.push(n);
  });

  const batcher = queue.batcher("record", { maxSize: 3, maxWaitMs: 60_000 });
  batcher.add([10]);
  batcher.add([20]);
  batcher.add([30]);

  const worker = queue.runWorker();
  try {
    expect(await waitFor(async () => seen.length === 3)).toBe(true);
    expect([...seen].sort((a, b) => a - b)).toEqual([10, 20, 30]);
  } finally {
    worker.stop();
  }
});

it("keeps entries buffered when a flush throws", () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 2, maxWaitMs: 60_000 });
  const enqueueMany = vi
    .spyOn(queue, "enqueueMany")
    .mockImplementationOnce(() => {
      throw new Error("storage down");
    })
    .mockImplementationOnce(() => ["job-1", "job-2"]);

  batcher.add([1]);
  expect(() => batcher.add([2])).toThrow("storage down");
  expect(batcher.size).toBe(2); // nothing dropped

  expect(batcher.flush()).toEqual(["job-1", "job-2"]);
  expect(batcher.size).toBe(0);
  enqueueMany.mockRestore();
});

it("reports a failed timed flush to onError and retries it", async () => {
  const queue = newQueue();
  const errors: unknown[] = [];
  const batcher = queue.batcher("collect", {
    maxSize: 100,
    maxWaitMs: 30,
    onError: (error) => errors.push(error),
  });
  const enqueueMany = vi.spyOn(queue, "enqueueMany").mockImplementationOnce(() => {
    throw new Error("storage down");
  });

  batcher.add([1]);
  expect(await waitFor(async () => errors.length > 0)).toBe(true);
  expect((errors[0] as Error).message).toBe("storage down");

  // The retry uses the real enqueueMany, so the entry lands rather than stranding.
  expect(await waitFor(async () => (await queue.stats()).pending === 1)).toBe(true);
  expect(batcher.size).toBe(0);
  enqueueMany.mockRestore();
  batcher.close();
});

it("rejects invalid tunables", () => {
  const queue = newQueue();
  expect(() => queue.batcher("collect", { maxSize: 0 })).toThrow(RangeError);
  expect(() => queue.batcher("collect", { maxSize: 1.5 })).toThrow(RangeError);
  expect(() => queue.batcher("collect", { maxWaitMs: 0 })).toThrow(RangeError);
  expect(() => queue.batcher("collect", { maxWaitMs: Number.NaN })).toThrow(RangeError);
  // Node clamps a delay past the 32-bit timer range to 1ms — an immediate flush
  // instead of the ~25-day wait that was asked for.
  expect(() => queue.batcher("collect", { maxWaitMs: 2_147_483_648 })).toThrow(RangeError);
  expect(() => queue.batcher("collect", { maxWaitMs: 2_147_483_647 })).not.toThrow();
});

it("contains a throwing onError and still retries the flush", async () => {
  const queue = newQueue();
  let calls = 0;
  const batcher = queue.batcher("collect", {
    maxSize: 100,
    maxWaitMs: 30,
    onError: () => {
      calls += 1;
      throw new Error("reporting blew up");
    },
  });
  const enqueueMany = vi.spyOn(queue, "enqueueMany").mockImplementationOnce(() => {
    throw new Error("storage down");
  });

  batcher.add([1]);
  expect(await waitFor(async () => calls > 0)).toBe(true);
  // The handler throwing must not skip the re-arm, so the entry still lands.
  expect(await waitFor(async () => (await queue.stats()).pending === 1)).toBe(true);
  expect(batcher.size).toBe(0);
  enqueueMany.mockRestore();
  batcher.close();
});

it("types add() from the registered task", () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 100 });

  // @ts-expect-error wrong argument types for the batched task
  batcher.add(["not a number"]);
  expect(batcher.size).toBe(1);
  batcher.close();
});

it("can be constructed directly against a queue", async () => {
  const queue = newQueue();
  const batcher = new Batcher(queue, "collect", { maxSize: 1 });
  expect(batcher.name).toBe("collect");
  expect(batcher.add([1])).toHaveLength(1);
  expect((await queue.stats()).pending).toBe(1);
});

it("stays consistent when a job.enqueued handler re-enters add", async () => {
  const queue = newQueue();
  const batcher = queue.batcher("collect", { maxSize: 2, maxWaitMs: 60_000 });
  let reentered = false;
  queue.on("job.enqueued", () => {
    if (!reentered) {
      reentered = true;
      batcher.add([99]); // fires while the first flush is still in enqueueMany
    }
  });

  batcher.add([1]);
  expect(batcher.add([2])).toHaveLength(2);
  expect(batcher.size).toBe(1); // only the re-entrant entry is left buffered
  expect((await queue.stats()).pending).toBe(2); // and it wasn't enqueued twice

  expect(batcher.close()).toHaveLength(1);
  expect((await queue.stats()).pending).toBe(3);
});
