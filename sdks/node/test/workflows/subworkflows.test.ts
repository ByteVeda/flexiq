import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker, type WorkflowEvent } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function freshQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-subwf-")), "queue.db");
  return new Queue({ dbPath });
}

function nodesByName<T extends { nodeName: string }>(handle: {
  nodes: () => T[];
}): Record<string, T | undefined> {
  return Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n]));
}

it("runs a child workflow as a node and advances the parent on completion", async () => {
  const queue = freshQueue();
  const ran: string[] = [];

  queue.task("prep", () => ran.push("prep"));
  queue.task("childA", () => ran.push("childA"));
  queue.task("childB", () => ran.push("childB"));
  queue.task("finish", () => ran.push("finish"));

  const child = queue.workflows
    .define("child-pipeline")
    .step("a", "childA")
    .step("b", "childB", { after: "a" })
    .build();

  const handle = queue.workflows
    .define("parent-pipeline")
    .step("prep", "prep")
    .subWorkflow("sub", { after: "prep", workflow: child })
    .step("finish", "finish", { after: "sub" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("completed");
  expect(ran).toEqual(["prep", "childA", "childB", "finish"]);
  const nodes = nodesByName(handle);
  expect(nodes.sub?.status).toBe("completed");
  expect(nodes.finish?.status).toBe("completed");

  const children = queue.workflows.children(handle.runId);
  expect(children).toHaveLength(1);
  expect(children[0]?.state).toBe("completed");
});

it("emits workflow.submitted for the child carrying parentRunId", async () => {
  const queue = freshQueue();
  const submitted: WorkflowEvent[] = [];
  queue.on("workflow.submitted", (event) => submitted.push(event));

  queue.task("prep", () => 1);
  queue.task("childA", () => 2);

  const child = queue.workflows.define("child-events").step("a", "childA").build();
  const handle = queue.workflows
    .define("parent-events")
    .step("prep", "prep")
    .subWorkflow("sub", { after: "prep", workflow: child })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });
  expect(run.state).toBe("completed");

  const parentEvent = submitted.find((event) => event.runId === handle.runId);
  expect(parentEvent).toEqual({ runId: handle.runId, name: "parent-events" });

  const childRunId = queue.workflows.children(handle.runId)[0]?.id;
  const childEvent = submitted.find((event) => event.runId === childRunId);
  expect(childEvent).toEqual({
    runId: childRunId,
    name: "child-events",
    parentRunId: handle.runId,
  });
});

it("fails the parent node and skips downstream when the child fails", async () => {
  const queue = freshQueue();

  queue.task("prep", () => 1);
  queue.task("boom", () => {
    throw new Error("child boom");
  });
  queue.task("finish", () => 2);

  const child = queue.workflows.define("child-fail").step("a", "boom", { maxRetries: 0 }).build();

  const handle = queue.workflows
    .define("parent-fail")
    .step("prep", "prep")
    .subWorkflow("sub", { after: "prep", workflow: child })
    .step("finish", "finish", { after: "sub" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("failed");
  const nodes = nodesByName(handle);
  expect(nodes.sub?.status).toBe("failed");
  expect(nodes.finish?.status).toBe("skipped");
  expect(queue.workflows.children(handle.runId)[0]?.state).toBe("failed");
});
