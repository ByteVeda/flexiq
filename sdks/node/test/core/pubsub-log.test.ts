// Log topics (S28): one stored message per publish, pulled via a cursor.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;
let activeQueue: Queue | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  // Await deregistration so scheduler threads don't accumulate into the next
  // test (a queue that never ran a worker returns instantly).
  if (activeQueue) {
    const q = activeQueue;
    activeQueue = undefined;
    await waitFor(async () => (await q.listWorkers()).length === 0, 30000);
  }
});

function newQueue(): Queue {
  activeQueue = new Queue({
    dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-pubsub-log-")), "q.db"),
  });
  return activeQueue;
}

// Huge ceilings: real-worker startup on a cold, CPU-starved smoke runner can
// take tens of seconds. The predicate returns the instant it's true.
const WORKER_TEST_TIMEOUT_MS = 60000;

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 40000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((r) => setTimeout(r, 20));
  }
  return false;
}

/** Narrow an array element to non-undefined (strict indexed access). */
function must<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("expected a value");
  return value;
}

it("stores one message per publish, regardless of readers", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "analytics");

  // No fan-out jobs; one stored message per publish.
  expect(await queue.publish("events", [1])).toEqual([]);
  expect(await queue.publish("events", [2])).toEqual([]);

  const msgs = await queue.readTopic("events", "analytics");
  expect(msgs.map((m) => m.args)).toEqual([[1], [2]]);
});

it("advances the cursor on ack and is monotonic", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  for (let i = 0; i < 3; i++) await queue.publish("events", [i]);

  const msgs = await queue.readTopic("events", "c");
  expect(msgs.map((m) => m.args[0])).toEqual([0, 1, 2]);

  // Ack through the middle: the next read starts after it.
  expect(await queue.ackTopic("events", "c", must(msgs[1]).id)).toBe(true);
  expect((await queue.readTopic("events", "c")).map((m) => m.args[0])).toEqual([2]);

  // Acking an older id never rewinds.
  expect(await queue.ackTopic("events", "c", must(msgs[0]).id)).toBe(false);
  expect((await queue.readTopic("events", "c")).map((m) => m.args[0])).toEqual([2]);
});

it("re-delivers un-acked messages (at-least-once)", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  await queue.publish("events", ["x"]);
  expect((await queue.readTopic("events", "c")).map((m) => m.args)).toEqual([["x"]]);
  expect((await queue.readTopic("events", "c")).map((m) => m.args)).toEqual([["x"]]);
});

it("bounds a read by limit", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  for (let i = 0; i < 5; i++) await queue.publish("events", [i]);

  const first = await queue.readTopic("events", "c", 2);
  expect(first.map((m) => m.args[0])).toEqual([0, 1]);
  await queue.ackTopic("events", "c", must(first[first.length - 1]).id);
  expect((await queue.readTopic("events", "c", 2)).map((m) => m.args[0])).toEqual([2, 3]);
});

it("reports lag per log subscription", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  for (let i = 0; i < 3; i++) await queue.publish("events", [i]);

  let stat = must((await queue.topicLogStats())[0]);
  expect(stat.subscription).toBe("c");
  expect(stat.cursor).toBeUndefined();
  expect(stat.lag).toBe(3);

  const msgs = await queue.readTopic("events", "c");
  await queue.ackTopic("events", "c", must(msgs[msgs.length - 1]).id);
  stat = must((await queue.topicLogStats())[0]);
  expect(stat.lag).toBe(0);
});

it("declareSubscriptions flushes a managed consumer's cursor before publishing", async () => {
  const queue = newQueue();
  // No worker: logConsumer registers the cursor eagerly but does not await it,
  // so declareSubscriptions() is the barrier that makes the next publish retain.
  queue.logConsumer("t-declare", "c", () => {});
  await queue.declareSubscriptions();

  await queue.publish("t-declare", [1]);
  expect((await queue.readTopic("t-declare", "c")).map((m) => m.args)).toEqual([[1]]);
});

it(
  "managed consumers: sync, async, retry-redeliver, and skip-poison in one worker",
  async () => {
    const queue = newQueue();
    const sync: number[] = [];
    const asyncSeen: number[] = [];
    const retry: number[] = [];
    const skip: number[] = [];
    let retryFailed = false;

    queue.logConsumer(
      "t-sync",
      "c",
      (n: number) => {
        sync.push(n);
      },
      { pollIntervalMs: 20 },
    );
    queue.logConsumer(
      "t-async",
      "c",
      async (n: number) => {
        await new Promise((r) => setTimeout(r, 1));
        asyncSeen.push(n);
      },
      { pollIntervalMs: 20 },
    );
    queue.logConsumer(
      "t-retry",
      "c",
      (n: number) => {
        retry.push(n);
        if (n === 1 && !retryFailed) {
          retryFailed = true;
          throw new Error("boom");
        }
      },
      { pollIntervalMs: 20, onError: "retry" },
    );
    queue.logConsumer(
      "t-skip",
      "c",
      (n: number) => {
        skip.push(n);
        if (n === 1) throw new Error("always poison");
      },
      { pollIntervalMs: 20, onError: "skip" },
    );

    // logConsumer() registers each cursor eagerly but does not await it, and a
    // publish before the log subscription exists is not retained — so declare
    // first, or a slow runner silently drops the earliest messages.
    await queue.declareSubscriptions();

    // One worker hosts all four consumers, so their poll loops share a single
    // (cold-runner-slow) startup instead of one worker lifecycle per test.
    worker = queue.runWorker({ concurrency: 1 });
    for (let i = 0; i < 3; i++) {
      await queue.publish("t-sync", [i]);
      await queue.publish("t-retry", [i]);
      await queue.publish("t-skip", [i]);
    }
    await queue.publish("t-async", [42]);

    expect(await waitFor(() => [...sync].sort().join() === "0,1,2")).toBe(true);
    expect(await waitFor(() => asyncSeen.length === 1)).toBe(true);
    expect(asyncSeen).toEqual([42]);
    // retry: msg 1 fails once then re-reads; msg 0, acked before the failure, isn't redelivered.
    expect(
      await waitFor(() => retry.filter((n) => n === 1).length === 2 && retry.includes(2)),
    ).toBe(true);
    expect(retry.filter((n) => n === 0).length).toBe(1);
    // skip: the poison is attempted once, then acked past.
    expect(await waitFor(() => [...skip].sort().join() === "0,1,2")).toBe(true);
    expect(skip.filter((n) => n === 1).length).toBe(1);
    const skipStat = must(
      (await queue.topicLogStats()).find((s) => s.topic === "t-skip" && s.subscription === "c"),
    );
    expect(skipStat.lag).toBe(0);
  },
  WORKER_TEST_TIMEOUT_MS,
);

it(
  "lets log and fan-out subscribers coexist on one topic",
  async () => {
    const queue = newQueue();
    const seen: number[] = [];
    queue.subscriber("events", "worker", (n: number) => {
      seen.push(n);
    });
    await queue.declareSubscriptions();
    await queue.subscribeLog("events", "log");

    worker = queue.runWorker({ concurrency: 1 });
    await queue.publish("events", [7]);

    expect(await waitFor(() => seen.length === 1)).toBe(true);
    expect(seen).toEqual([7]);
    // The same publish stored one log message.
    expect((await queue.readTopic("events", "log")).map((m) => m.args)).toEqual([[7]]);
  },
  WORKER_TEST_TIMEOUT_MS,
);
