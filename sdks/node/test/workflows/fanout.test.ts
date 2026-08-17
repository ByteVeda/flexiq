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

function freshQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-fanout-")), "queue.db");
  return new Queue({ dbPath });
}

/** Map a finished run's nodes by name for compact assertions. */
function nodesByName<T extends { nodeName: string }>(handle: {
  nodes: () => T[];
}): Record<string, T | undefined> {
  return Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n]));
}

it("fans out over a producer's items and collects them in a fan-in", async () => {
  const queue = freshQueue();
  const processed: number[] = [];
  let combined: number[] | undefined;

  queue.task("source", () => [1, 2, 3]);
  queue.task("double", (n: number) => {
    processed.push(n);
    return n * 2;
  });
  queue.task("sum", (results: number[]) => {
    combined = results;
    return results.reduce((acc, x) => acc + x, 0);
  });

  const handle = queue.workflows
    .define("fanout-etl")
    .step("source", "source")
    .fanOut("process", { after: "source", task: "double", itemsFrom: "source" })
    .fanIn("collect", { after: "process", task: "sum" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("completed");
  expect(processed.slice().sort((a, b) => a - b)).toEqual([1, 2, 3]);
  expect(combined?.slice().sort((a, b) => a - b)).toEqual([2, 4, 6]);

  const nodes = nodesByName(handle);
  expect(nodes.process?.status).toBe("completed");
  expect(nodes.process?.fanOutCount).toBe(3);
  expect(nodes.collect?.status).toBe("completed");
  // One child node was created per item.
  expect(
    ["process[0]", "process[1]", "process[2]"].every((n) => nodes[n]?.status === "completed"),
  ).toBe(true);
});

it("fails the run and skips the fan-in when a fan-out child dead-letters", async () => {
  const queue = freshQueue();
  queue.task("source", () => [1, 2, 3]);
  queue.task("maybeBoom", (n: number) => {
    if (n === 2) {
      throw new Error("boom");
    }
    return n;
  });
  queue.task("collect", (r: number[]) => r);

  const handle = queue.workflows
    .define("fanout-fail")
    .step("source", "source")
    .fanOut("process", { after: "source", task: "maybeBoom", itemsFrom: "source", maxRetries: 0 })
    .fanIn("collect", { after: "process", task: "collect" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("failed");
  const nodes = nodesByName(handle);
  expect(nodes.process?.status).toBe("failed");
  expect(nodes.collect?.status).toBe("skipped");
});

it("completes a fan-out over an empty list and runs the fan-in with []", async () => {
  const queue = freshQueue();
  let combined: unknown = "unset";

  queue.task("source", () => []);
  queue.task("noop", (n: number) => n);
  queue.task("collect", (r: unknown[]) => {
    combined = r;
    return r.length;
  });

  const handle = queue.workflows
    .define("fanout-empty")
    .step("source", "source")
    .fanOut("process", { after: "source", task: "noop", itemsFrom: "source" })
    .fanIn("collect", { after: "process", task: "collect" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 8_000 });

  expect(run.state).toBe("completed");
  expect(combined).toEqual([]);
  const nodes = nodesByName(handle);
  expect(nodes.process?.status).toBe("completed");
  expect(nodes.process?.fanOutCount).toBe(0);
  expect(nodes.collect?.status).toBe("completed");
});

it("runs a fan-out without a fan-in collector", async () => {
  const queue = freshQueue();
  const seen: number[] = [];

  queue.task("source", () => [10, 20]);
  queue.task("handle", (n: number) => {
    seen.push(n);
    return n;
  });

  const handle = queue.workflows
    .define("fanout-no-collect")
    .step("source", "source")
    .fanOut("process", { after: "source", task: "handle", itemsFrom: "source" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 8_000 });

  expect(run.state).toBe("completed");
  expect(seen.slice().sort((a, b) => a - b)).toEqual([10, 20]);
  expect(nodesByName(handle).process?.fanOutCount).toBe(2);
});

it("runs a fan-out's sibling successor alongside its fan-in", async () => {
  const queue = freshQueue();
  let combined: number[] | undefined;
  let notified = false;

  queue.task("source", () => [1, 2]);
  queue.task("double", (n: number) => n * 2);
  queue.task("sum", (results: number[]) => {
    combined = results;
    return results.length;
  });
  queue.task("notify", () => {
    notified = true;
  });

  const handle = queue.workflows
    .define("fanout-sibling")
    .step("source", "source")
    .fanOut("process", { after: "source", task: "double", itemsFrom: "source" })
    .fanIn("collect", { after: "process", task: "sum" })
    .step("notify", "notify", { after: "process", condition: "always" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("completed");
  expect(notified).toBe(true);
  expect(combined?.slice().sort((a, b) => a - b)).toEqual([2, 4]);
  const nodes = nodesByName(handle);
  expect(nodes.collect?.status).toBe("completed");
  expect(nodes.notify?.status).toBe("completed");
});

it("rejects a fan-in whose target is not a fan-out step", () => {
  const queue = freshQueue();
  expect(() =>
    queue.workflows
      .define("bad-fanin")
      .step("plain", "noop")
      .fanIn("collect", { after: "plain", task: "sum" })
      .build(),
  ).toThrow(/must target a fan-out step/);
});
