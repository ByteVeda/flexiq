/**
 * End-to-end tests for the attached executor.
 *
 * A scheduler is played by `FakeScheduler`, a plain socket speaking the frame
 * protocol, so these run without building the Rust server binary.
 * `executorAttachServer.test.ts` runs the same assertions against the real
 * `flexiq-server` when one is available — that pairing is what keeps this fake
 * honest. Durable steps over the same attach live in `steps.test.ts`, beside
 * the worker's.
 */

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { DETACHED_ENV } from "../../src/detached";
import { currentJob, DetachedStorageError, type Executor, Queue } from "../../src/index";
import { FakeScheduler, PROTOCOL_VERSION, SETTLE_MS, sleep } from "./fakeScheduler";

let executor: Executor | undefined;
let scheduler: FakeScheduler | undefined;

afterEach(async () => {
  await executor?.stop();
  executor = undefined;
  scheduler?.close();
  scheduler = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-exec-")), "q.db") });
}

/** Uses the job-scoped conveniences that need storage in a worker. */
async function reportingHandler(): Promise<string> {
  const job = currentJob();
  job?.setProgress(50);
  job?.publish({ stage: "halfway" });
  job?.setProgress(100);
  return "reported";
}

/** The queue's job-scoped writes, which on an executor have no storage behind them. */
interface JobScopedWrites {
  updateProgress(jobId: string, progress: number): void;
  writeTaskLog(
    jobId: string,
    taskName: string,
    level: string,
    message: string,
    extra?: string,
  ): void;
}

/**
 * Reach the queue's native handle.
 *
 * Private on purpose, and the seam under test on purpose: this is what a task
 * uses when it goes through the queue rather than `currentJob()`, and on an
 * executor it has to reach the scheduler just the same.
 */
function nativeOf(queue: Queue): JobScopedWrites {
  return (queue as unknown as { native: JobScopedWrites }).native;
}

/** Encode a call the way the enqueue path does. */
function payloadFor(queue: Queue, args: unknown[]): Buffer {
  // biome-ignore lint/complexity/useLiteralKeys: reaching the internal serializer
  const serializer = (queue as unknown as { serializer: { serialize(v: unknown): Uint8Array } })[
    "serializer"
  ];
  return Buffer.from(serializer.serialize([args, {}]));
}

it("announces itself and the tasks it can run", async () => {
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("echo", (value: string) => `echo:${value}`);
  queue.task("boom", () => {
    throw new Error("deliberate failure");
  });

  executor = await queue.runExecutor({
    attach: `127.0.0.1:${scheduler.port}`,
    slots: 2,
    executorId: "exec-test",
  });
  const hello = await scheduler.attached();

  expect(hello.executor_id).toBe("exec-test");
  expect(hello.sdk).toBe("node");
  expect(hello.slots).toBe(2);
  expect(hello.protocol_version).toBe(PROTOCOL_VERSION);
  // Only advertised tasks are ever dispatched, so a missing name here is a job
  // that silently never runs.
  expect(hello.tasks).toEqual(expect.arrayContaining(["echo", "boom"]));
  // A token that was never configured must not appear on the wire.
  expect(hello).not.toHaveProperty("token");
  // Claimed only where the job context can open a session; `steps.test.ts`
  // covers what the scheduler then sends.
  expect(hello.capabilities).toEqual(expect.arrayContaining(["steps"]));
  expect(executor.schedulerId).toBe("fake-scheduler");
});

it("runs a dispatched job and returns its result", async () => {
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("echo", (value: string) => `echo:${value}`);

  executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
  await scheduler.attached();
  scheduler.sendJob("job-1", "echo", payloadFor(queue, ["hello"]));

  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("success");
  expect(frame.header.job_id).toBe("job-1");
  expect(frame.payload.length).toBeGreaterThan(0);
});

it("reports a task failure with its retry verdict", async () => {
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("boom", () => {
    throw new Error("deliberate failure");
  });

  executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
  await scheduler.attached();
  scheduler.sendJob("job-1", "boom", payloadFor(queue, []), { retryCount: 2 });

  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("failure");
  expect(frame.header.job_id).toBe("job-1");
  expect(frame.header.should_retry).toBe(true);
  expect(frame.header.timed_out).toBe(false);
  // The frame's retry count is echoed back so the scheduler's backoff is right.
  expect(frame.header.retry_count).toBe(2);
  expect(String(frame.header.error)).toContain("deliberate failure");
});

it("honours a task's retryOn predicate over the wire", async () => {
  // Only the executor sees the exception, so its verdict is the one that
  // counts; a wire defaulting this to true would retry poison jobs forever.
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task(
    "fatal",
    () => {
      throw new Error("do not retry me");
    },
    { retryOn: () => false },
  );

  executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
  await scheduler.attached();
  scheduler.sendJob("job-1", "fatal", payloadFor(queue, []));

  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("failure");
  expect(frame.header.should_retry).toBe(false);
});

it("runs jobs on separate slots concurrently", async () => {
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  let running = 0;
  let peak = 0;
  let release = (): void => {};
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  queue.task("slow", async () => {
    running += 1;
    peak = Math.max(peak, running);
    await released;
    running -= 1;
    return null;
  });

  executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}`, slots: 2 });
  await scheduler.attached();
  scheduler.sendJob("job-1", "slow", payloadFor(queue, []));
  scheduler.sendJob("job-2", "slow", payloadFor(queue, []));

  const deadline = Date.now() + SETTLE_MS;
  while (peak < 2 && Date.now() < deadline) {
    await sleep(10);
  }
  release();

  const first = await scheduler.nextResult();
  const second = await scheduler.nextResult();
  expect([first.header.job_id, second.header.job_id].sort()).toEqual(["job-1", "job-2"]);
  expect(peak).toBe(2);
});

it("announces zero capacity before disconnecting on stop", async () => {
  // This is what makes the drain clean rather than a race: the scheduler is
  // told to stop dispatching in-protocol, before the connection goes away.
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("echo", (value: string) => value);

  const running = await queue.runExecutor({
    attach: `127.0.0.1:${scheduler.port}`,
    slots: 2,
    heartbeatIntervalMs: 50,
  });
  await scheduler.attached();

  const stopping = running.stop();
  await scheduler.heartbeat(0);
  await stopping;
  executor = undefined;
});

it("finishes in-flight work before disconnecting", async () => {
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  let release = (): void => {};
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  let started = false;
  queue.task("slow", async () => {
    started = true;
    await released;
    return "done";
  });

  const running = await queue.runExecutor({
    attach: `127.0.0.1:${scheduler.port}`,
    heartbeatIntervalMs: 50,
  });
  await scheduler.attached();
  scheduler.sendJob("job-1", "slow", payloadFor(queue, []));

  const deadline = Date.now() + SETTLE_MS;
  while (!started && Date.now() < deadline) {
    await sleep(10);
  }

  // Stop while the job is still running: it must still report, or the job
  // waits for a reap it never needed.
  const stopping = running.stop();
  release();
  const frame = await scheduler.nextResult();
  expect(frame.header.type).toBe("success");
  expect(frame.header.job_id).toBe("job-1");

  await stopping;
  executor = undefined;
});

it("ends the session when the scheduler sends shutdown", async () => {
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("echo", (value: string) => value);

  const running = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
  await scheduler.attached();
  expect(running.running).toBe(true);

  scheduler.send({ type: "shutdown" });
  await running.wait();

  expect(running.running).toBe(false);
  await running.stop();
  executor = undefined;
});

it("reports a refused attach rather than a network error", async () => {
  // A wrong token is the likeliest deployment mistake; it must not surface as
  // "connection reset" and send the operator looking at the network.
  scheduler = await FakeScheduler.listen({ refuse: true });
  const queue = newQueue();
  queue.task("echo", (value: string) => value);

  await expect(
    queue.runExecutor({ attach: `127.0.0.1:${scheduler?.port}`, token: "wrong-token" }),
  ).rejects.toThrow(/refused|token/i);
});

it("fails fast when no scheduler is listening", async () => {
  const queue = newQueue();
  queue.task("echo", (value: string) => value);

  // Port 1 on loopback is reserved and nothing listens there.
  await expect(queue.runExecutor({ attach: "127.0.0.1:1", connectTimeoutMs: 500 })).rejects.toThrow(
    /could not reach the scheduler/,
  );
});

it("requires an attach address", async () => {
  const queue = newQueue();
  queue.task("echo", (value: string) => value);

  const previous = process.env.FLEXIQ_ATTACH;
  process.env.FLEXIQ_ATTACH = undefined;
  delete process.env.FLEXIQ_ATTACH;
  try {
    await expect(queue.runExecutor()).rejects.toThrow(/FLEXIQ_ATTACH/);
  } finally {
    if (previous !== undefined) {
      process.env.FLEXIQ_ATTACH = previous;
    }
  }
});

it("takes the attach address and slot count from the environment", async () => {
  scheduler = await FakeScheduler.listen();
  const queue = newQueue();
  queue.task("echo", (value: string) => value);

  const previousAttach = process.env.FLEXIQ_ATTACH;
  const previousSlots = process.env.FLEXIQ_SLOTS;
  process.env.FLEXIQ_ATTACH = `127.0.0.1:${scheduler.port}`;
  process.env.FLEXIQ_SLOTS = "3";
  try {
    executor = await queue.runExecutor();
    const hello = await scheduler.attached();
    expect(hello.slots).toBe(3);
  } finally {
    restoreEnv("FLEXIQ_ATTACH", previousAttach);
    restoreEnv("FLEXIQ_SLOTS", previousSlots);
  }
});

it("rejects a non-numeric slot count from the environment", async () => {
  const queue = newQueue();
  queue.task("echo", (value: string) => value);

  const previous = process.env.FLEXIQ_SLOTS;
  process.env.FLEXIQ_SLOTS = "many";
  try {
    await expect(queue.runExecutor({ attach: "127.0.0.1:1" })).rejects.toThrow(RangeError);
  } finally {
    restoreEnv("FLEXIQ_SLOTS", previous);
  }
});

function restoreEnv(name: string, previous: string | undefined): void {
  if (previous === undefined) {
    delete process.env[name];
  } else {
    process.env[name] = previous;
  }
}

it("opens no storage", async () => {
  // The point of the attach split: app code without database credentials.
  // Pointed at a Postgres DSN nothing is listening on — a Queue that connected
  // could not even be constructed.
  scheduler = await FakeScheduler.listen();
  process.env[DETACHED_ENV] = "1";
  try {
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.task("echo", (value: string) => `echo:${value}`);

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();

    // And it still runs jobs.
    scheduler.sendJob("job-1", "echo", payloadFor(queue, ["detached"]));
    const frame = await scheduler.nextResult();
    expect(frame.header.type).toBe("success");
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("degrades progress and publish rather than failing the job", async () => {
  // This scheduler advertises no side-channel — an older `flexiq-server` —
  // so the executor sends nothing it could not parse. Losing the progress bar
  // is a degradation; failing the job over it would be a regression for anyone
  // moving a worker to an executor.
  scheduler = await FakeScheduler.listen();
  process.env[DETACHED_ENV] = "1";
  try {
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.task("reports", reportingHandler);

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();
    scheduler.sendJob("job-1", "reports", payloadFor(queue, []));

    const { result, sideChannel } = await scheduler.collectUntilResult();
    expect(result.header.type).toBe("success");
    expect(result.header.job_id).toBe("job-1");
    expect(sideChannel).toEqual([]);
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("sends progress and logs to a scheduler that advertised the side-channel", async () => {
  // The whole point of #589: a task on an executor is not silently poorer than
  // the same task on an in-process worker.
  scheduler = await FakeScheduler.listen();
  scheduler.capabilities = ["side_channel"];
  process.env[DETACHED_ENV] = "1";
  try {
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.task("reports", reportingHandler);

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();
    scheduler.sendJob("job-1", "reports", payloadFor(queue, []));

    const { result, sideChannel } = await scheduler.collectUntilResult();
    expect(result.header.type).toBe("success");

    const progress = sideChannel
      .filter((frame) => frame.header.type === "progress")
      .map((frame) => frame.header.progress);
    expect(progress.at(-1)).toBe(100);

    const partial = sideChannel.find((frame) => frame.header.level === "result");
    expect(partial).toBeDefined();
    expect(partial?.header.job_id).toBe("job-1");
    expect(partial?.header.task_name).toBe("reports");
    expect(JSON.parse(partial?.payload.toString() ?? "")).toEqual({ stage: "halfway" });
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("sends the queue's own progress and log writes to the scheduler", async () => {
  // The other seam: a task that reaches the queue directly rather than through
  // `currentJob()`. On an executor that shim has no storage behind it, and
  // warning-and-dropping there would make the same call silently poorer than
  // the context one.
  scheduler = await FakeScheduler.listen();
  scheduler.capabilities = ["side_channel"];
  process.env[DETACHED_ENV] = "1";
  try {
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.task("reports", () => {
      const native = nativeOf(queue);
      native.updateProgress("job-1", 25);
      native.writeTaskLog("job-1", "reports", "warning", "from the queue", '{"via":"queue"}');
      return "reported";
    });

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();
    scheduler.sendJob("job-1", "reports", payloadFor(queue, []));

    const { result, sideChannel } = await scheduler.collectUntilResult();
    expect(result.header.type).toBe("success");

    const progress = sideChannel
      .filter((frame) => frame.header.type === "progress")
      .map((frame) => frame.header.progress);
    expect(progress).toContain(25);

    const logged = sideChannel.find((frame) => frame.header.level === "warning");
    expect(logged?.header.message).toBe("from the queue");
    expect(logged?.header.task_name).toBe("reports");
    expect(JSON.parse(logged?.payload.toString() ?? "")).toEqual({ via: "queue" });
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("writes a structured log line from the job context", async () => {
  // `publish` was the only route to a task log before this; a plain log line
  // had nowhere to go on an executor at all.
  scheduler = await FakeScheduler.listen();
  scheduler.capabilities = ["side_channel"];
  process.env[DETACHED_ENV] = "1";
  try {
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.task("logs", () => {
      currentJob()?.log("halfway", "warning", { step: 2 });
      return "logged";
    });

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();
    scheduler.sendJob("job-1", "logs", payloadFor(queue, []));

    const { result, sideChannel } = await scheduler.collectUntilResult();
    expect(result.header.type).toBe("success");

    const logged = sideChannel.find((frame) => frame.header.type === "task_log");
    expect(logged?.header.level).toBe("warning");
    expect(logged?.header.message).toBe("halfway");
    expect(logged?.header.job_id).toBe("job-1");
    expect(JSON.parse(logged?.payload.toString() ?? "")).toEqual({ step: 2 });
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("skips a middleware the dispatch says is disabled", async () => {
  // A dashboard toggle has to reach a process that cannot read settings, so it
  // rides the job frame instead.
  scheduler = await FakeScheduler.listen();
  process.env[DETACHED_ENV] = "1";
  try {
    const ran: string[] = [];
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.use({ name: "recorder", before: () => void ran.push("recorder") });
    queue.task("echo", (value: string) => `echo:${value}`);

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();

    scheduler.sendJob("job-1", "echo", payloadFor(queue, ["a"]));
    expect((await scheduler.nextResult()).header.type).toBe("success");
    expect(ran).toEqual(["recorder"]);

    scheduler.sendJob("job-2", "echo", payloadFor(queue, ["b"]), {
      disabledMiddleware: ["recorder"],
    });
    expect((await scheduler.nextResult()).header.type).toBe("success");
    expect(ran).toEqual(["recorder"]);
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("honours the toggles on a job dispatched in the same tick as the ack", async () => {
  // The earliest a scheduler can dispatch: the job frame follows the ack with
  // nothing in between. The native attach starts its job loop before
  // `runExecutor` resolves, so an invocation here can outrun the holder the
  // callback reads the executor from — and reading it empty would run a
  // middleware the dispatch said was disabled.
  scheduler = await FakeScheduler.listen();
  process.env[DETACHED_ENV] = "1";
  try {
    const ran: string[] = [];
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.use({ name: "recorder", before: () => void ran.push("recorder") });
    queue.task("echo", (value: string) => `echo:${value}`);

    const payload = payloadFor(queue, ["a"]);
    scheduler.afterAck = () => {
      scheduler?.sendJob("job-1", "echo", payload, { disabledMiddleware: ["recorder"] });
    };

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });

    expect((await scheduler.nextResult()).header.type).toBe("success");
    expect(ran).toEqual([]);
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("refuses a storage operation instead of silently dropping it", async () => {
  // An enqueue that quietly vanished would be worse than one that threw.
  process.env[DETACHED_ENV] = "1";
  try {
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    queue.task("echo", (value: string) => value);
    expect(() => queue.enqueue("echo", ["x"])).toThrow(DetachedStorageError);
  } finally {
    delete process.env[DETACHED_ENV];
  }
});

it("aborts a running handler when a cancel frame arrives", async () => {
  // A detached executor has no storage flag to poll, so the cancel has to reach
  // the handler's AbortSignal from the frame the scheduler sent.
  scheduler = await FakeScheduler.listen();
  process.env[DETACHED_ENV] = "1";
  try {
    const queue = new Queue({ backend: "postgres", dsn: "postgres://x:y@127.0.0.1:1/absent" });
    let started = false;
    queue.task("slow", async () => {
      started = true;
      const job = currentJob();
      for (let i = 0; i < 400; i += 1) {
        if (job?.signal.aborted) {
          throw new Error("cancelled");
        }
        await sleep(25);
      }
      return "never";
    });

    executor = await queue.runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();
    scheduler.sendJob("job-1", "slow", payloadFor(queue, []));

    const deadline = Date.now() + SETTLE_MS;
    while (!started && Date.now() < deadline) {
      await sleep(10);
    }
    scheduler.send({ type: "cancel", job_id: "job-1" });

    const frame = await scheduler.nextResult();
    // `cancelled`, not `failure`: the handler observed the signal and threw,
    // and the native side reclassified that throw as the cancellation it was.
    // Both halves of the frame-driven cancel path in one assertion.
    expect(frame.header.type).toBe("cancelled");
    expect(frame.header.job_id).toBe("job-1");
  } finally {
    delete process.env[DETACHED_ENV];
  }
});
