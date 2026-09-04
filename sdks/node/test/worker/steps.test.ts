/**
 * Durable inline steps: `ctx.step.run` and `ctx.step.sleep`.
 *
 * Two deployments, one set of rules. On a **worker** everything is asserted
 * through the public surface — events, `queue.getJob` and counters closed over
 * by the handlers. Nothing here opens the queue's database: a second SQLite in
 * the test process does not share the worker's WAL index, so it reads a table
 * the worker has already written as empty, with no error to say so.
 *
 * On an **attached executor** there is no database on this side at all, so the
 * assertions are the frames themselves: the snapshot a replay answers from
 * rides in on the dispatch, and every new step crosses to the scheduler and
 * blocks on its acknowledgement. `FakeScheduler` plays that end.
 */

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import {
  currentJob,
  type Duration,
  type Executor,
  type Middleware,
  type OutcomeEvent,
  Queue,
  type SleepEvent,
  type StepContext,
  StepUnavailableError,
  type Worker,
} from "../../src/index";
import { FakeScheduler } from "./fakeScheduler";

let worker: Worker | undefined;
let executor: Executor | undefined;
let scheduler: FakeScheduler | undefined;

afterEach(async () => {
  // Awaited: `stop()` resolves once the native worker has quiesced and its
  // resources are disposed, and a test starting before that shares a database
  // with the last one's threads.
  await worker?.stop();
  worker = undefined;
  await executor?.stop();
  executor = undefined;
  scheduler?.close();
  scheduler = undefined;
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

async function waitFor(predicate: () => boolean, timeoutMs = 20_000): Promise<boolean> {
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
  // Far enough ahead that worker startup cannot consume the window and turn the
  // sleep into an `Elapsed` replay, which commits no sleep and emits no event.
  // Only `sleepUntil` is exposed to this: a relative duration is read on the
  // worker, at step time.
  const deadline = new Date(Date.now() + 2_000);

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
  expect(sleeping.queue).toBe("default");
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

it("fences each of a queue's workers on its own claim", async () => {
  // The owner half of `(owner, attempt)` belongs to the worker that won the
  // claim, not to the queue they were started from. Held on the queue, a second
  // `runWorker` would overwrite the first worker's id and every step it went on
  // to commit would be refused as superseded — safe, but the job dies.
  const queue = newQueue();
  const completed: OutcomeEvent[] = [];
  const dead: OutcomeEvent[] = [];
  const charged: string[] = [];

  queue.on("job.completed", (event) => completed.push(event));
  queue.on("job.dead", (event) => dead.push(event));
  queue.task("checkout", async (label: string) => {
    return step().run("charge", () => {
      charged.push(label);
      return label;
    });
  });

  const first = queue.runWorker({ queues: ["alpha"] });
  const second = queue.runWorker({ queues: ["beta"] });
  try {
    queue.enqueue("checkout", ["on-alpha"], { queue: "alpha" });
    queue.enqueue("checkout", ["on-beta"], { queue: "beta" });

    expect(await waitFor(() => completed.length === 2)).toBe(true);
    expect(dead).toHaveLength(0);
    expect(charged.sort()).toEqual(["on-alpha", "on-beta"]);
  } finally {
    first.stop();
    second.stop();
  }
});

it("dead-letters invalid step input rather than retrying it", async () => {
  // A missing name and an unparseable duration are deterministic — the replay
  // is handed the same value — so §9.2 calls them permanent. Retried, they
  // would spend the whole budget to reach the same dead letter.
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  const retries: OutcomeEvent[] = [];

  queue.on("job.dead", (event) => dead.push(event));
  queue.on("job.retrying", (event) => retries.push(event));
  queue.task(
    "unnamed",
    async () => {
      return step().run("", () => "value");
    },
    RETRIES,
  );
  queue.task(
    "badsleep",
    async () => {
      // Cast because `Duration` is a template-literal type, so TypeScript
      // rejects this spelling outright. The runtime path is still reachable
      // from JavaScript, and from any duration read out of config.
      return step().sleep("1 hour" as Duration);
    },
    RETRIES,
  );

  queue.enqueue("unnamed");
  queue.enqueue("badsleep");

  worker = queue.runWorker();

  expect(await waitFor(() => dead.length === 2)).toBe(true);
  expect(retries).toHaveLength(0);
  expect(dead.map((event) => String(event.error)).join("\n")).toContain("a step needs a name");
});

it("latches invalid step input, so catching it cannot report a result", async () => {
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  const completed: OutcomeEvent[] = [];

  queue.on("job.dead", (event) => dead.push(event));
  queue.on("job.completed", (event) => completed.push(event));
  queue.task(
    "swallows",
    async () => {
      try {
        await step().run("", () => "value");
      } catch {
        // the body carries on as if the step had never been asked for
      }
      return "done";
    },
    RETRIES,
  );

  queue.enqueue("swallows");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  expect(completed).toHaveLength(0);
  expect(String(dead[0]?.error)).toContain("caught a step control signal");
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

// ── On an attached executor ──────────────────────────────────────────────
//
// No storage on this side: the steps a job already committed arrive as a
// `job_steps` frame in front of its dispatch, and each new one crosses as a
// `step_commit` the task waits on. The fence stays the scheduler's, so nothing
// below sends an owner and nothing below could.

/** Reach the queue's own serializer — the one a step result is encoded with. */
interface QueueSerializer {
  serialize(value: unknown): Uint8Array;
  deserialize(bytes: Uint8Array): unknown;
}

function serializerOf(queue: Queue): QueueSerializer {
  return (queue as unknown as { serializer: QueueSerializer }).serializer;
}

/** Encode a call the way the enqueue path does. */
function payloadFor(queue: Queue, args: unknown[]): Buffer {
  return Buffer.from(serializerOf(queue).serialize([args, {}]));
}

/** Attach an executor to `scheduler` and wait for the handshake. */
async function attach(queue: Queue, fake: FakeScheduler): Promise<Executor> {
  const started = await queue.runExecutor({ attach: `127.0.0.1:${fake.port}` });
  await fake.attached();
  return started;
}

it("announces that it can run durable steps", async () => {
  // Only an executor whose job context can actually open a session may claim
  // this: a scheduler sends the snapshot to nobody who would discard it.
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("checkout", () => "done");

  executor = await attach(queue, scheduler);
  const hello = await scheduler.attached();

  expect(hello.capabilities).toEqual(expect.arrayContaining(["steps"]));
});

it("replays a step from the dispatch's snapshot instead of running it", async () => {
  // The read half of §9: one snapshot per attempt, and it is the scheduler's
  // read, not one this process has credentials to make.
  scheduler = await FakeScheduler.listen();
  scheduler.capabilities = ["steps"];
  const queue = newQueue();
  let charges = 0;
  queue.task("checkout", async () => {
    return step().run("charge", () => {
      charges += 1;
      return { id: `ch_${charges}` };
    });
  });

  executor = await attach(queue, scheduler);
  const memoized = serializerOf(queue).serialize({ id: "ch_1" });
  scheduler.sendJobSteps("job-1", [{ seq: 0, stepKey: "charge#0", result: Buffer.from(memoized) }]);
  scheduler.sendJob("job-1", "checkout", payloadFor(queue, []));

  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("success");
  // The point of the whole feature, over a wire this time: the card is not
  // charged again on the attempt that replays it.
  expect(charges).toBe(0);
  expect(serializerOf(queue).deserialize(frame.payload)).toEqual({ id: "ch_1" });
});

it("commits a new step through the scheduler and waits for the ack", async () => {
  scheduler = await FakeScheduler.listen();
  scheduler.capabilities = ["steps"];
  const queue = newQueue();
  queue.task("checkout", async () => step().run("charge", () => ({ id: "ch_1" })));

  executor = await attach(queue, scheduler);
  scheduler.sendJob("job-1", "checkout", payloadFor(queue, []));

  const commit = await scheduler.nextFrame("step_commit");
  expect(commit.header.job_id).toBe("job-1");
  expect(commit.header.seq).toBe(0);
  expect(commit.header.step_key).toBe("charge#0");
  expect(commit.header.kind).toBe("run");
  // Post-serializer, post-codec: these are the bytes the scheduler stores, and
  // the ones a replay hands back. No owner rides with them.
  expect(serializerOf(queue).deserialize(commit.payload)).toEqual({ id: "ch_1" });
  expect(commit.header).not.toHaveProperty("owner");

  scheduler.ackStep(commit);
  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("success");
});

it("ends the attempt in a sleep the scheduler settled", async () => {
  // Two frames, not one: `step.sleep` has to return the deadline storage
  // settled on before the body unwinds, and the terminal frame is only written
  // once it has.
  scheduler = await FakeScheduler.listen();
  scheduler.capabilities = ["steps"];
  const queue = newQueue();
  const settled = Date.now() + 90_000;
  queue.task("cool_off", async () => {
    await step().sleep("1h", { name: "cool_off" });
    return "never reached this attempt";
  });

  executor = await attach(queue, scheduler);
  scheduler.sendJob("job-1", "cool_off", payloadFor(queue, []));

  const commit = await scheduler.nextFrame("step_commit");
  expect(commit.header.kind).toBe("sleep");
  expect(commit.header.step_key).toBe("cool_off#0");
  expect(commit.header.payload_len).toBe(0);
  // The ack echoes the deadline the job was *actually* rescheduled to, which on
  // a replay is the stored one rather than the one proposed here.
  scheduler.ackStep(commit, { wakeAt: settled });

  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("slept");
  expect(frame.header.wake_at).toBe(settled);
});

it("fails the attempt retryably when a commit is never acknowledged", async () => {
  // The one genuinely uncertain case in §9.2's taxonomy. An unconfirmed commit
  // is indistinguishable from one that never happened, so the attempt must not
  // proceed past it — and the replay re-runs the step under the same downstream
  // idempotency key, which is what makes retrying safe.
  scheduler = await FakeScheduler.listen();
  scheduler.capabilities = ["steps"];
  const queue = newQueue();
  let refusal: unknown;
  queue.task("checkout", async () => {
    try {
      return await step().run("charge", () => ({ id: "ch_1" }));
    } catch (error) {
      refusal = error;
      throw error;
    }
  });

  executor = await attach(queue, scheduler);
  scheduler.sendJob("job-1", "checkout", payloadFor(queue, []));

  // Asserted in the handler rather than off a frame: the connection carrying
  // the answer is the one being dropped, so there is no failure frame to read.
  const commit = await scheduler.nextFrame("step_commit");
  expect(commit.header.step_key).toBe("charge#0");
  scheduler.disconnect();

  expect(await waitFor(() => refusal !== undefined)).toBe(true);
  expect(refusal).toBeInstanceOf(StepUnavailableError);
  expect(refusal).toMatchObject({ flexiqShouldRetry: true });
});

it("refuses a step when the scheduler offers no step store", async () => {
  // §9.4, and it stays: an executor attached to a scheduler that never
  // advertised the capability has no channel to commit on, and says so.
  // Retryably, because a fleet mid-rollout may place the next attempt somewhere
  // that can commit — and there is no version of "your charge step silently
  // lost its memo" that beats a failure naming the reason.
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("checkout", async () => step().run("charge", () => "charged"));

  executor = await attach(queue, scheduler);
  scheduler.sendJob("job-1", "checkout", payloadFor(queue, []));

  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("failure");
  expect(frame.header.should_retry).toBe(true);
  expect(String(frame.header.error)).toContain("no step store");
});
