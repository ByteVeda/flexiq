/**
 * A middleware hook that outruns its budget is abandoned, not fatal.
 *
 * A task's own timeout bounds its handler and nothing else, so a `before` that
 * blocks holds the attempt open past that limit. The unit tests below pin what
 * the bound does to one hook; the worker test pins that the chain is actually
 * wired to it, by hanging a `before` forever and still getting a result.
 */

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import {
  type LogLevel,
  type Middleware,
  type OutcomeEvent,
  Queue,
  setLogLevel,
  setLogSink,
  type Worker,
} from "../../src/index";
import { validateMiddlewareTimeoutMs, withHookDeadline } from "../../src/middleware-deadline";

let worker: Worker | undefined;
const lines: string[] = [];

afterEach(async () => {
  await worker?.stop();
  worker = undefined;
  lines.length = 0;
  setLogSink((_level, line) => process.stderr.write(`${line}\n`));
  setLogLevel((process.env.FLEXIQ_LOG_LEVEL as LogLevel | undefined) ?? "warn");
});

/** Capture every line the logger emits for the rest of the test. */
function captureLogs(): void {
  setLogLevel("warn");
  setLogSink((_level, line) => lines.push(line));
}

const overruns = () => lines.filter((line) => line.includes("exceeded"));

const never = () => new Promise<void>(() => {});

const after = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

it("stops awaiting a hook that outruns its budget, and names it", async () => {
  captureLogs();

  const started = Date.now();
  await withHookDeadline(20, "Slow", "before", never);

  // The wait ended; the hook itself is still out there, which is the point.
  expect(Date.now() - started).toBeLessThan(1000);
  expect(overruns()).toHaveLength(1);
  expect(overruns()[0]).toContain("middleware Slow before() exceeded 20ms; abandoned");
});

it("lets a hook inside its budget through untouched", async () => {
  captureLogs();

  let ran = false;
  await withHookDeadline(1000, "Fast", "after", async () => {
    await after(1);
    ran = true;
  });

  expect(ran).toBe(true);
  expect(overruns()).toEqual([]);
});

it("still fails the attempt when a hook rejects before its deadline", async () => {
  captureLogs();

  // Only the overrun is swallowed. A hook that throws on its own keeps the
  // behaviour it had before this bound existed, or a `before` that means to
  // reject a job would be silently ignored.
  await expect(
    withHookDeadline(1000, "Angry", "before", () => Promise.reject(new Error("nope"))),
  ).rejects.toThrow("nope");
  expect(overruns()).toEqual([]);
});

it("waits forever when the budget is disabled", async () => {
  captureLogs();

  let resolved = false;
  const pending = withHookDeadline(0, "Unbounded", "before", async () => {
    await after(50);
    resolved = true;
  });
  expect(resolved).toBe(false);
  await pending;

  expect(resolved).toBe(true);
  expect(overruns()).toEqual([]);
});

it("rejects a budget it could never honour", () => {
  // `NaN > 0` is false, so it would silently disable the bound; Node normalizes
  // an out-of-range setTimeout delay to 1ms, so `Infinity` would expire every
  // hook rather than none. Both fail quietly, so the constructor refuses them.
  for (const bad of [Number.NaN, Number.POSITIVE_INFINITY, -1]) {
    expect(() => new Queue({ middlewareTimeoutMs: bad })).toThrow(RangeError);
  }
  expect(() => validateMiddlewareTimeoutMs(undefined)).not.toThrow();
  expect(validateMiddlewareTimeoutMs(0)).toBe(0);
});

it("runs the task even when a middleware's before never returns", async () => {
  captureLogs();
  const queue = new Queue({
    dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-mw-deadline-")), "q.db"),
    middlewareTimeoutMs: 20,
  });
  const completed: OutcomeEvent[] = [];
  queue.on("job.completed", (event) => completed.push(event));

  const hung: Middleware = { name: "Hung", before: never };
  queue.use(hung);
  queue.task("bounded", async () => "ran");

  const jobId = queue.enqueue("bounded");
  worker = queue.runWorker();

  const deadline = Date.now() + 10_000;
  while (completed.length === 0 && Date.now() < deadline) {
    await after(20);
  }

  expect(queue.getResult(jobId)).toBe("ran");
  expect(overruns().some((line) => line.includes("middleware Hung before()"))).toBe(true);
});
