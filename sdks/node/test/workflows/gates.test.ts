import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { type GateEvent, Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function freshQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-gate-")), "queue.db");
  return new Queue({ dbPath });
}

function nodesByName<T extends { nodeName: string }>(handle: {
  nodes: () => T[];
}): Record<string, T | undefined> {
  return Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n]));
}

async function waitFor(predicate: () => boolean, timeoutMs = 6000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("pauses at a gate and runs downstream once approved", async () => {
  const queue = freshQueue();
  let released = false;

  queue.task("prepare", () => 1);
  queue.task("ship", () => {
    released = true;
  });

  const handle = queue.workflows
    .define("gate-approve")
    .step("prepare", "prepare")
    .gate("review", { after: "prepare" })
    .step("ship", "ship", { after: "review" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });

  // The run parks at the gate until approved.
  expect(await waitFor(() => nodesByName(handle).review?.status === "waiting_approval")).toBe(true);
  expect(released).toBe(false);
  expect(handle.status()?.state).toBe("running");

  queue.workflows.approveGate(handle.runId, "review");

  const run = await handle.wait({ timeoutMs: 8000 });
  expect(run.state).toBe("completed");
  expect(released).toBe(true);
  expect(nodesByName(handle).review?.status).toBe("completed");
});

it("skips downstream and fails the run when a gate is rejected", async () => {
  const queue = freshQueue();
  queue.task("prepare", () => 1);
  queue.task("ship", () => 2);

  const handle = queue.workflows
    .define("gate-reject")
    .step("prepare", "prepare")
    .gate("review", { after: "prepare" })
    .step("ship", "ship", { after: "review" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  expect(await waitFor(() => nodesByName(handle).review?.status === "waiting_approval")).toBe(true);

  queue.workflows.rejectGate(handle.runId, "review", "not allowed");

  const run = await handle.wait({ timeoutMs: 8000 });
  expect(run.state).toBe("failed");
  const nodes = nodesByName(handle);
  expect(nodes.review?.status).toBe("failed");
  expect(nodes.ship?.status).toBe("skipped");
});

it("starts saga compensation when a gate is rejected", async () => {
  const queue = freshQueue();
  const rollbacks: string[] = [];

  queue.task("reserve", () => "reservation");
  queue.task("ship", () => 1);
  queue.task("unreserve", (forward: string) => rollbacks.push(`unreserve:${forward}`));

  const handle = queue.workflows
    .define("gate-reject-saga")
    .step("reserve", "reserve", { compensate: "unreserve" })
    .gate("review", { after: "reserve" })
    .step("ship", "ship", { after: "review" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  expect(await waitFor(() => nodesByName(handle).review?.status === "waiting_approval")).toBe(true);

  queue.workflows.rejectGate(handle.runId, "review", "not allowed");

  const run = await handle.wait({ timeoutMs: 8000 });
  expect(run.state).toBe("compensated");
  expect(rollbacks).toEqual(["unreserve:reservation"]);
  expect(nodesByName(handle).reserve?.status).toBe("compensated");
});

it("resolves the parent node when a gate inside a sub-workflow is approved", async () => {
  const queue = freshQueue();
  const ran: string[] = [];

  queue.task("prep", () => ran.push("prep"));
  queue.task("childTask", () => ran.push("childTask"));
  queue.task("finish", () => ran.push("finish"));

  const child = queue.workflows
    .define("child-gated")
    .step("a", "childTask")
    .gate("approval", { after: "a" })
    .build();

  const handle = queue.workflows
    .define("parent-gated")
    .step("prep", "prep")
    .subWorkflow("sub", { after: "prep", workflow: child })
    .step("finish", "finish", { after: "sub" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });

  expect(await waitFor(() => queue.workflows.children(handle.runId).length > 0)).toBe(true);
  const childId = queue.workflows.children(handle.runId)[0]?.id ?? "";
  expect(
    await waitFor(() =>
      queue.workflows
        .nodes(childId)
        .some((n) => n.nodeName === "approval" && n.status === "waiting_approval"),
    ),
  ).toBe(true);

  queue.workflows.approveGate(childId, "approval");

  const run = await handle.wait({ timeoutMs: 10_000 });
  expect(run.state).toBe("completed");
  expect(nodesByName(handle).sub?.status).toBe("completed");
  expect(ran).toEqual(["prep", "childTask", "finish"]);
});

it("emits workflow.gate_reached with the gate message", async () => {
  const queue = freshQueue();
  const reached: GateEvent[] = [];
  queue.on("workflow.gate_reached", (event) => reached.push(event));
  queue.task("prepare", () => 1);

  const handle = queue.workflows
    .define("gate-event")
    .step("prepare", "prepare")
    .gate("review", { after: "prepare", message: "please review" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });

  expect(await waitFor(() => reached.length > 0)).toBe(true);
  expect(reached).toEqual([{ runId: handle.runId, node: "review", message: "please review" }]);

  queue.workflows.approveGate(handle.runId, "review");
  const run = await handle.wait({ timeoutMs: 8000 });
  expect(run.state).toBe("completed");
  expect(reached).toHaveLength(1); // approval does not re-enter the gate
});

it("auto-resolves a gate on timeout per onTimeout", async () => {
  const queue = freshQueue();
  let shipped = false;

  queue.task("prepare", () => 1);
  queue.task("ship", () => {
    shipped = true;
  });

  const handle = queue.workflows
    .define("gate-timeout")
    .step("prepare", "prepare")
    .gate("review", { after: "prepare", timeoutMs: 200, onTimeout: "approve" })
    .step("ship", "ship", { after: "review" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 8000 });

  expect(run.state).toBe("completed"); // timeout approved the gate
  expect(shipped).toBe(true);
  expect(nodesByName(handle).review?.status).toBe("completed");
});
