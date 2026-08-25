/**
 * Durable inline steps on a worker: `ctx.step.run` and `ctx.step.sleep`.
 *
 * Everything is asserted through the public surface — events, `queue.getJob`
 * and counters closed over by the handlers. Nothing here opens the queue's
 * database: a second SQLite in the test process does not share the worker's
 * WAL index, so it reads a table the worker has already written as empty, with
 * no error to say so.
 */

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import {
  currentJob,
  type Middleware,
  type OutcomeEvent,
  Queue,
  type SleepEvent,
  type StepContext,
  StepUnavailableError,
  type Worker,
} from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-steps-")), "q.db") });
}

/** The running job's step context. Throws outside a task, which is a test bug. */
function step(): StepContext {
  const job = currentJob();
  if (!job) {
    throw new Error("no job context — this handler is not running on a worker");
  }
  return job.step;
}

async function waitFor(predicate: () => boolean, timeoutMs = 10_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

/** Fast retries, and enough of them that a dead-letter is a verdict, not a budget. */
const RETRIES = { maxRetries: 5, retryBackoff: { baseMs: 1, maxMs: 10 } } as const;

it("replays a committed step instead of running it again", async () => {
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const runKeys: string[] = [];
  let charges = 0;
  let attempts = 0;

  queue.on("job.completed", (event) => completed.push(event));
  queue.task(
    "checkout",
    async () => {
      runKeys.push(await step().runKey());
      const charge = await step().run("charge", () => {
        charges += 1;
        return { id: `ch_${charges}` };
      });
      attempts += 1;
      if (attempts === 1) {
        throw new Error("downstream blip after the charge");
      }
      return charge.id;
    },
    RETRIES,
  );

  const jobId = queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  expect(attempts).toBe(2);
  // The point of the whole feature: the card is charged once across two attempts.
  expect(charges).toBe(1);
  expect(queue.getResult(jobId)).toBe("ch_1");
  expect(runKeys).toEqual([jobId, jobId]);
});

it("answers whether its backend can memoize a step at all", () => {
  // A backend that answered `false` would refuse every step rather than degrade
  // to "no steps recorded" — that answer re-runs a charge.
  expect(newQueue().supportsSteps()).toBe(true);
});

it("matches a keyed step by its key, wherever it has moved to", async () => {
  // The escape hatch for a step whose position is not stable — a loop over an
  // unordered collection. Matched positionally, the replay's first step would
  // land on the other one's row and diverge.
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const charged: string[] = [];
  const replayed: string[] = [];
  let attempts = 0;

  queue.on("job.completed", (event) => completed.push(event));
  queue.task(
    "fanout",
    async () => {
      attempts += 1;
      const items = attempts === 1 ? ["alice", "bob"] : ["bob", "alice"];
      for (const item of items) {
        const memo = await step().run(
          "charge",
          () => {
            charged.push(item);
            return item;
          },
          { key: item },
        );
        if (attempts > 1) {
          replayed.push(memo);
        }
      }
      if (attempts === 1) {
        throw new Error("force a replay in the other order");
      }
      return "done";
    },
    RETRIES,
  );

  queue.enqueue("fanout");
  worker = queue.runWorker();

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  expect(attempts).toBe(2);
  // Both steps were memo hits on the replay: neither callback ran again.
  expect(charged).toEqual(["alice", "bob"]);
  // And each key got *its own* row back. Matched by position instead, the
  // replay's first step ("bob") would have been handed alice's charge.
  expect(replayed).toEqual(["bob", "alice"]);
});

it("dead-letters two steps started at once rather than interleaving them", async () => {
  // A step's identity is its position in the sequence, so a second one started
  // while the first is uncommitted has no position to take.
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  const retries: OutcomeEvent[] = [];

  queue.on("job.dead", (event) => dead.push(event));
  queue.on("job.retrying", (event) => retries.push(event));
  queue.task(
    "checkout",
    async () => {
      const steps = step();
      await Promise.all([steps.run("charge", () => 1), steps.run("receipt", () => 2)]);
      return "done";
    },
    RETRIES,
  );

  queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  expect(retries).toHaveLength(0);
  expect(String(dead[0]?.error)).toContain("still uncommitted");
});

it("sleeps until an absolute instant", async () => {
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const slept: SleepEvent[] = [];
  const deadline = new Date(Date.now() + 300);

  queue.on("job.completed", (event) => completed.push(event));
  queue.on("job.sleeping", (event) => slept.push(event));
  queue.task("billing", async () => {
    await step().sleepUntil(deadline, { name: "cycle" });
    return "billed";
  });

  queue.enqueue("billing");
  worker = queue.runWorker();

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  expect(slept).toHaveLength(1);
  // The instant the caller named, not one derived from when the attempt ran.
  expect(slept[0]?.wakeAt).toBe(deadline.getTime());
});

it("ends the attempt on a sleep and replays earlier steps on wake", async () => {
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const slept: SleepEvent[] = [];
  let reserved = 0;
  let receipts = 0;

  queue.on("job.completed", (event) => completed.push(event));
  queue.on("job.sleeping", (event) => slept.push(event));
  queue.task("checkout", async () => {
    const hold = await step().run("reserve", () => {
      reserved += 1;
      return "hold-1";
    });
    await step().sleep("400ms", { name: "settle" });
    receipts += 1;
    return hold;
  });

  const jobId = queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => slept.length > 0)).toBe(true);
  const sleeping = slept[0] as SleepEvent;
  expect(sleeping.jobId).toBe(jobId);
  expect(sleeping.stepKey).toBe("settle#0");
  expect(sleeping.wakeAt).toBeGreaterThan(Date.now());
  // A sleeping job holds no worker slot: it is pending at its deadline, not
  // running, so it cannot be timed out while it waits.
  expect(queue.getJob(jobId)?.status).toBe("pending");

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  // The step before the sleep is a memo hit on wake, and the body past it ran
  // only in the attempt that finished.
  expect(reserved).toBe(1);
  expect(receipts).toBe(1);
  // A sleep is not a retry.
  expect(queue.getJob(jobId)?.retryCount).toBe(0);
});

it("keeps the first deadline when a sleep is replayed", async () => {
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const slept: SleepEvent[] = [];
  let attempts = 0;

  queue.on("job.completed", (event) => completed.push(event));
  queue.on("job.sleeping", (event) => slept.push(event));
  queue.task(
    "checkout",
    async () => {
      await step().sleep("200ms", { name: "settle" });
      attempts += 1;
      if (attempts === 1) {
        throw new Error("blip after the sleep elapsed");
      }
      return "done";
    },
    RETRIES,
  );

  queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  expect(attempts).toBe(2);
  // Two attempts ran past the sleep, and only the first one slept. A sleep
  // re-issued on every replay would have pushed the deadline out again and
  // emitted a second event — which is how a crash loop outlives its job.
  expect(slept).toHaveLength(1);
});

it("mints a stable downstream key for the step that is running", async () => {
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const keys: string[] = [];

  queue.on("job.completed", (event) => completed.push(event));
  queue.task(
    "checkout",
    async () => {
      await step().run("charge", () => {
        keys.push(step().idempotencyKey);
        if (keys.length === 1) {
          // Fails before the commit, so the step is new ground again next time
          // and its key can be compared across two real runs.
          throw new Error("the charge call timed out");
        }
        return "ok";
      });
      return "done";
    },
    RETRIES,
  );

  const jobId = queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  expect(keys).toHaveLength(2);
  expect(keys[0]).toBe(`${jobId}:charge#0`);
  // The whole point: the retried call carries the key the first one did, so the
  // downstream service dedupes it.
  expect(keys[1]).toBe(keys[0]);
});

it("refuses to read the downstream key outside a step body", async () => {
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];

  queue.on("job.dead", (event) => dead.push(event));
  queue.task(
    "checkout",
    async () => {
      return step().idempotencyKey;
    },
    RETRIES,
  );

  queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  expect(String(dead[0]?.error)).toContain("only readable inside a step body");
});

it("dead-letters a divergence without spending the retry budget on it", async () => {
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  const retries: OutcomeEvent[] = [];
  let flipped = false;

  queue.on("job.dead", (event) => dead.push(event));
  queue.on("job.retrying", (event) => retries.push(event));
  queue.task(
    "checkout",
    async () => {
      await step().run(flipped ? "refund" : "charge", () => "value");
      if (!flipped) {
        flipped = true;
        throw new Error("force one retry, so the next attempt replays");
      }
      return "done";
    },
    RETRIES,
  );

  queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  // One retry — the forced one. The divergence itself is not retried: the code
  // asking will be just as wrong next attempt.
  expect(retries).toHaveLength(1);
  expect(String(dead[0]?.error)).toContain("charge");
});

it("fails an attempt whose body swallowed a divergence", async () => {
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  const completed: OutcomeEvent[] = [];
  let flipped = false;

  queue.on("job.dead", (event) => dead.push(event));
  queue.on("job.completed", (event) => completed.push(event));
  queue.task(
    "checkout",
    async () => {
      try {
        await step().run(flipped ? "refund" : "charge", () => "value");
      } catch {
        // Exactly what the latch exists for: JavaScript has no error a `catch`
        // misses, so the runner has to notice afterwards.
      }
      if (!flipped) {
        flipped = true;
        throw new Error("force one retry, so the next attempt replays");
      }
      return "swallowed and carried on";
    },
    RETRIES,
  );

  queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  expect(completed).toHaveLength(0);
  expect(String(dead[0]?.error)).toContain("caught a step control signal");
});

it("still wakes a job whose body swallowed its sleep", async () => {
  // The latch fires here too, and the scheduler then drops the failure: the
  // sleep row is committed and the claim released, so `(owner, attempt)` reads
  // the attempt as superseded. One attempt is wasted; nothing is broken. A
  // test that used a sleep to "prove the latch" would be proving nothing.
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  let past = 0;

  queue.on("job.completed", (event) => completed.push(event));
  queue.task("checkout", async () => {
    try {
      await step().sleep("200ms", { name: "settle" });
    } catch {
      // swallowed
    }
    past += 1;
    return "done";
  });

  const jobId = queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  // Twice: the swallowing attempt, then the one that woke and finished.
  expect(past).toBe(2);
  expect(queue.getJob(jobId)?.retryCount).toBe(0);
});

it("pairs a middleware's before with onSleep rather than after", async () => {
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const calls: string[] = [];

  const probe: Middleware = {
    name: "probe",
    before: () => {
      calls.push("before");
    },
    after: () => {
      calls.push("after");
    },
    onSleep: (_ctx, wakeAt) => {
      calls.push(wakeAt > 0 ? "onSleep" : "onSleep:bad-deadline");
    },
  };

  queue.on("job.completed", (event) => completed.push(event));
  queue.use(probe);
  queue.task("checkout", async () => {
    await step().sleep("200ms", { name: "settle" });
    return "done";
  });

  queue.enqueue("checkout");
  worker = queue.runWorker();

  expect(await waitFor(() => completed.length > 0)).toBe(true);
  // The slept attempt gets `onSleep` and no `after`, because an attempt that
  // has not finished has no result for `after` to see.
  expect(calls).toEqual(["before", "onSleep", "before", "after"]);
});

it("refuses a step where nothing can commit it", async () => {
  // The shape an attached executor is in: no store, so the step fails rather
  // than running un-memoized, and fails *retryably* — a heterogeneous fleet
  // mid-rollout may put the next attempt on a worker that can commit.
  const { StepContext: Ctx, StepLatch } = await import("../../src/steps");
  const { JsonSerializer } = await import("../../src/serializers");

  const latch = new StepLatch();
  const context = new Ctx("job-1", 0, new JsonSerializer(), latch);

  await expect(context.run("charge", () => "value")).rejects.toBeInstanceOf(StepUnavailableError);
  await expect(context.run("charge", () => "value")).rejects.toMatchObject({
    flexiqShouldRetry: true,
  });
  // Latched, so a body that caught the refusal and returned anyway still fails.
  expect(latch.swallowed).toBe(true);
});
