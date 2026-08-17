import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker, WorkflowError } from "../../src/index";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-wfcache-")), "q.db") });
}

it("reuses a cacheable step's result across runs", async () => {
  const queue = newQueue();
  let runs = 0;
  queue.task("seed", () => 10);
  queue.task("expensive", (x: number) => {
    runs += 1;
    return x * 2;
  });
  worker = queue.runWorker();

  const build = () =>
    queue.workflows
      .define("wf")
      .step("seed", "seed")
      .step("expensive", "expensive", { after: "seed", args: [10], cache: true });

  await build().submit().wait();
  expect(runs).toBe(1);

  const handle = build().submit();
  const run = await handle.wait();
  expect(run.state).toBe("completed");
  expect(runs).toBe(1); // cache hit — `expensive` did not re-run

  const node = queue.workflows.nodes(handle.runId).find((n) => n.nodeName === "expensive");
  expect(queue.getResult(node?.jobId as string)).toBe(20); // result intact on a hit
});

it("misses when an upstream result changes (dirty set)", async () => {
  const queue = newQueue();
  let runs = 0;
  queue.task("seed", (n: number) => n);
  queue.task("expensive", (x: number) => {
    runs += 1;
    return x;
  });
  worker = queue.runWorker();

  const run = (seedValue: number) =>
    queue.workflows
      .define("wf")
      .step("seed", "seed", { args: [seedValue] })
      .step("expensive", "expensive", { after: "seed", args: [0], cache: true })
      .submit()
      .wait();

  await run(1);
  expect(runs).toBe(1);
  await run(2); // seed result changed → `expensive`'s key differs → miss
  expect(runs).toBe(2);
});

it("re-runs after clearCache", async () => {
  const queue = newQueue();
  let runs = 0;
  queue.task("seed", () => 1);
  queue.task("expensive", () => {
    runs += 1;
    return 1;
  });
  worker = queue.runWorker();

  const build = () =>
    queue.workflows
      .define("wf")
      .step("seed", "seed")
      .step("expensive", "expensive", { after: "seed", cache: true });

  await build().submit().wait();
  expect(runs).toBe(1);
  expect(queue.workflows.clearCache()).toBeGreaterThan(0);
  await build().submit().wait();
  expect(runs).toBe(2); // cache cleared → re-run
});

it("rejects a cacheable root step at build time", () => {
  const queue = newQueue();
  expect(() => queue.workflows.define("wf").step("a", "noop", { cache: true }).submit()).toThrow(
    WorkflowError,
  );
});
