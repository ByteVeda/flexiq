import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue } from "../../src/index";

function newQueue(): Queue {
  const queue = new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-canvas-")), "q.db") });
  queue.task("noop", () => undefined);
  return queue;
}

it("chain wires steps sequentially", () => {
  const queue = newQueue();
  const handle = queue.workflows
    .define("c")
    .chain([
      { name: "a", task: "noop" },
      { name: "b", task: "noop" },
      { name: "d", task: "noop" },
    ])
    .submit();
  const a = queue.workflows.analyze(handle.runId);
  expect(a?.roots()).toEqual(["a"]);
  expect(a?.leaves()).toEqual(["d"]);
  expect(a?.topologicalLevels()).toEqual([["a"], ["b"], ["d"]]);
});

it("group runs steps in parallel", () => {
  const queue = newQueue();
  const handle = queue.workflows
    .define("g")
    .group([
      { name: "a", task: "noop" },
      { name: "b", task: "noop" },
      { name: "d", task: "noop" },
    ])
    .submit();
  const a = queue.workflows.analyze(handle.runId);
  expect(a?.roots().sort()).toEqual(["a", "b", "d"]);
  expect(a?.leaves().sort()).toEqual(["a", "b", "d"]);
});

it("chord joins a group with a callback", () => {
  const queue = newQueue();
  const handle = queue.workflows
    .define("ch")
    .chord(
      [
        { name: "a", task: "noop" },
        { name: "b", task: "noop" },
      ],
      { name: "cb", task: "noop" },
    )
    .submit();
  const a = queue.workflows.analyze(handle.runId);
  expect(a?.roots().sort()).toEqual(["a", "b"]);
  expect(a?.leaves()).toEqual(["cb"]);
  expect(a?.ancestors("cb").sort()).toEqual(["a", "b"]);
});

it("chains after an existing step", () => {
  const queue = newQueue();
  const handle = queue.workflows
    .define("seeded")
    .step("seed", "noop")
    .chain(
      [
        { name: "a", task: "noop" },
        { name: "b", task: "noop" },
      ],
      { after: "seed" },
    )
    .submit();
  const a = queue.workflows.analyze(handle.runId);
  expect(a?.roots()).toEqual(["seed"]);
  expect(a?.criticalPath()).toEqual(["seed", "a", "b"]);
});
