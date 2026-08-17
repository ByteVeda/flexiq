import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue, WorkflowAnalysis } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-wf-an-")), "q.db") });
}

/** Diamond: a → b, a → c, b → d, c → d. */
function submitDiamond(queue: Queue): string {
  queue.task("noop", () => undefined);
  const handle = queue.workflows
    .define("diamond")
    .step("a", "noop")
    .step("b", "noop", { after: "a" })
    .step("c", "noop", { after: "a" })
    .step("d", "noop", { after: ["b", "c"] })
    .submit();
  return handle.runId;
}

it("returns undefined for an unknown run", () => {
  const queue = newQueue();
  expect(queue.workflows.analyze("nope")).toBeUndefined();
});

it("computes roots, leaves, ancestors and descendants", () => {
  const queue = newQueue();
  const analysis = queue.workflows.analyze(submitDiamond(queue));
  expect(analysis).toBeInstanceOf(WorkflowAnalysis);
  if (!analysis) {
    return;
  }
  expect(analysis.roots()).toEqual(["a"]);
  expect(analysis.leaves()).toEqual(["d"]);
  expect(analysis.ancestors("d").sort()).toEqual(["a", "b", "c"]);
  expect(analysis.descendants("a").sort()).toEqual(["b", "c", "d"]);
  expect(analysis.ancestors("a")).toEqual([]);
});

it("groups nodes into topological levels", () => {
  const queue = newQueue();
  const analysis = queue.workflows.analyze(submitDiamond(queue));
  const levels = analysis?.topologicalLevels() ?? [];
  expect(levels[0]).toEqual(["a"]);
  expect(levels[1]?.sort()).toEqual(["b", "c"]);
  expect(levels[2]).toEqual(["d"]);
});

it("finds the critical path (longest dependency chain)", () => {
  const queue = newQueue();
  const analysis = queue.workflows.analyze(submitDiamond(queue));
  const path = analysis?.criticalPath() ?? [];
  expect(path).toHaveLength(3);
  expect(path[0]).toBe("a");
  expect(path[2]).toBe("d");
  expect(["b", "c"]).toContain(path[1]);
});

it("reports node-status stats", () => {
  const queue = newQueue();
  const analysis = queue.workflows.analyze(submitDiamond(queue));
  const stats = analysis?.stats();
  expect(stats?.total).toBe(4);
  expect(stats?.completed).toBe(0); // no worker ran
  expect(stats?.failed).toBe(0);
});

it("counts cache_hit and compensated as terminal, not pending", () => {
  const graph = {
    nodes: [{ name: "a" }, { name: "b" }, { name: "c" }],
    edges: [
      { from: "a", to: "b" },
      { from: "b", to: "c" },
    ],
  };
  const nodes = [
    { runId: "r", nodeName: "a", status: "cache_hit" },
    { runId: "r", nodeName: "b", status: "compensated" },
    { runId: "r", nodeName: "c", status: "compensation_failed" },
  ];
  const stats = new WorkflowAnalysis(graph, nodes).stats();
  expect(stats.pending).toBe(0);
  expect(stats.completed).toBe(1); // cache_hit is a success
  expect(stats.failed).toBe(2); // compensated + compensation_failed
});
