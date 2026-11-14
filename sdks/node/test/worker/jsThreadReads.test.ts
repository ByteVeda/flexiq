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

/** Count every call into the native queue, by method name, while `on` is set. */
function countNativeCalls(queue: Queue): { counts: Map<string, number>; on: { value: boolean } } {
  const native = (queue as unknown as { native: object }).native;
  const proto = Object.getPrototypeOf(native) as Record<string, unknown>;
  const counts = new Map<string, number>();
  const on = { value: false };
  for (const name of Object.getOwnPropertyNames(proto)) {
    const fn = proto[name];
    if (typeof fn !== "function" || name === "constructor") {
      continue;
    }
    // Patched on this instance only, so other tests' queues are untouched.
    Object.defineProperty(native, name, {
      value(...args: unknown[]) {
        if (on.value) {
          counts.set(name, (counts.get(name) ?? 0) + 1);
        }
        return (fn as (...a: unknown[]) => unknown).apply(native, args);
      },
    });
  }
  return { counts, on };
}

it("runs a plain job with no storage reads on the JS thread (#962)", async () => {
  // Native queue calls are synchronous on the JS thread. On a networked backend
  // each is a round trip during which no other in-flight job can start, resolve
  // or report — the worker used to make four per job (the middleware disable
  // list twice, and two job reads to classify the outcome for workflows).
  const queue = new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-jsr-")), "q.db") });
  queue.task("noop", (i: number) => i);
  queue.use({ before() {} });
  const jobs = 40;
  for (let i = 0; i < jobs; i += 1) {
    queue.enqueue("noop", [i]);
  }
  let completed = 0;
  const drained = new Promise<void>((resolve) => {
    queue.on("job.completed", () => {
      completed += 1;
      if (completed === jobs) {
        resolve();
      }
    });
  });

  const { counts, on } = countNativeCalls(queue);
  on.value = true;
  worker = queue.runWorker({ concurrency: 4 });
  await drained;
  on.value = false;

  // Startup and the heartbeat make a handful of calls; per-job reads would
  // make at least one per job.
  const total = [...counts.values()].reduce((sum, n) => sum + n, 0);
  expect(total, JSON.stringify(Object.fromEntries(counts))).toBeLessThan(jobs / 4);
});
