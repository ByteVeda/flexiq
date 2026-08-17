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
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-wf-")), "queue.db");
  return new Queue({ dbPath });
}

it("runs a linear DAG to completion in dependency order", async () => {
  const queue = freshQueue();
  const ran: string[] = [];
  queue.task("extract", () => {
    ran.push("extract");
    return { rows: 3 };
  });
  queue.task("transform", () => {
    ran.push("transform");
    return "done";
  });

  const handle = queue.workflows
    .define("etl")
    .step("extract", "extract")
    .step("transform", "transform", { after: "extract" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 8000 });

  expect(run.state).toBe("completed");
  expect(ran).toEqual(["extract", "transform"]);
  const status = Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n.status]));
  expect(status).toEqual({ extract: "completed", transform: "completed" });
});

it("fails the run and skips downstream steps when a step dead-letters", async () => {
  const queue = freshQueue();
  queue.task("ok", () => 1);
  queue.task("boom", () => {
    throw new Error("kaboom");
  });
  queue.task("after", () => 2);

  const handle = queue.workflows
    .define("failing")
    .step("a", "ok")
    .step("b", "boom", { after: "a", maxRetries: 0 })
    .step("c", "after", { after: "b" })
    .submit();

  worker = queue.runWorker();
  const run = await handle.wait({ timeoutMs: 8000 });

  expect(run.state).toBe("failed");
  const status = Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n.status]));
  expect(status.a).toBe("completed");
  expect(status.b).toBe("failed");
  expect(status.c).toBe("skipped");
});

it("skips a static successor of a failed step in a managed run", async () => {
  const queue = freshQueue();
  const ran: string[] = [];
  queue.task("boom", () => {
    throw new Error("kaboom");
  });
  queue.task("after", () => ran.push("after"));
  queue.task("recover", () => ran.push("recover"));

  // `recover`'s condition makes the run tracker-managed, which suppresses the
  // core's fail-fast cascade — the tracker must settle `next` itself.
  const handle = queue.workflows
    .define("managed-static-cascade")
    .step("risky", "boom", { maxRetries: 0 })
    .step("next", "after", { after: "risky" })
    .step("recover", "recover", { after: "risky", condition: "on_failure" })
    .submit();

  worker = queue.runWorker();
  const run = await handle.wait({ timeoutMs: 8000 });

  expect(run.state).toBe("failed");
  const status = Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n.status]));
  expect(status.risky).toBe("failed");
  expect(status.next).toBe("skipped");
  expect(status.recover).toBe("completed");
  expect(ran).toEqual(["recover"]);
});

// 30s budget: a cold Windows CI runner has blown the global 15s on this test.
it("emits workflow.submitted and one workflow.completed", { timeout: 30_000 }, async () => {
  const queue = freshQueue();
  const submitted: WorkflowEvent[] = [];
  const completed: WorkflowEvent[] = [];
  queue.on("workflow.submitted", (event) => submitted.push(event));
  queue.on("workflow.completed", (event) => completed.push(event));
  queue.task("noop", () => 1);

  const handle = queue.workflows.define("events-happy").step("only", "noop").submit();
  expect(submitted).toEqual([{ runId: handle.runId, name: "events-happy" }]);

  worker = queue.runWorker();
  await handle.wait({ timeoutMs: 8000 });
  // Let any trailing outcome callbacks land before checking for duplicates.
  await new Promise((resolve) => setTimeout(resolve, 200));

  expect(completed).toHaveLength(1);
  expect(completed[0]).toEqual({
    runId: handle.runId,
    state: "completed",
    error: undefined,
  });
});

it("emits workflow.failed once for a failing run", async () => {
  const queue = freshQueue();
  const failed: WorkflowEvent[] = [];
  queue.on("workflow.failed", (event) => failed.push(event));
  queue.task("boom", () => {
    throw new Error("kaboom");
  });

  const handle = queue.workflows
    .define("events-failing")
    .step("only", "boom", { maxRetries: 0 })
    .submit();

  worker = queue.runWorker();
  const run = await handle.wait({ timeoutMs: 8000 });
  await new Promise((resolve) => setTimeout(resolve, 200));

  expect(run.state).toBe("failed");
  expect(failed).toHaveLength(1);
  expect(failed[0]?.runId).toBe(handle.runId);
  expect(failed[0]?.state).toBe("failed");
  expect(failed[0]?.error).toBeTruthy();
});

it("lists submitted runs and filters by state", async () => {
  const queue = freshQueue();
  queue.task("noop", () => null);

  const handle = queue.workflows.define("listme").step("only", "noop").submit();
  worker = queue.runWorker();
  await handle.wait({ timeoutMs: 8000 });

  const completed = queue.workflows.list({ state: "completed" });
  expect(completed.map((r) => r.id)).toContain(handle.runId);
});
