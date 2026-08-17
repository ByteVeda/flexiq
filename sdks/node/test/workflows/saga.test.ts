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
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-saga-")), "queue.db");
  return new Queue({ dbPath });
}

function nodesByName<T extends { nodeName: string }>(handle: {
  nodes: () => T[];
}): Record<string, T | undefined> {
  return Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n]));
}

it("compensates completed steps in reverse order when a later step fails", async () => {
  const queue = freshQueue();
  const rollbacks: string[] = [];

  queue.task("reserve", () => "reservation");
  queue.task("charge", () => "charge-id");
  queue.task("ship", () => {
    throw new Error("out of stock");
  });
  queue.task("unreserve", (forward: string) => rollbacks.push(`unreserve:${forward}`));
  queue.task("refund", (forward: string) => rollbacks.push(`refund:${forward}`));

  const handle = queue.workflows
    .define("saga-order")
    .step("reserve", "reserve", { compensate: "unreserve" })
    .step("charge", "charge", { after: "reserve", compensate: "refund" })
    .step("ship", "ship", { after: "charge", maxRetries: 0 })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("compensated");
  // charge rolls back before reserve (reverse dependency order).
  expect(rollbacks).toEqual(["refund:charge-id", "unreserve:reservation"]);
  const nodes = nodesByName(handle);
  expect(nodes.reserve?.status).toBe("compensated");
  expect(nodes.charge?.status).toBe("compensated");
  expect(nodes.ship?.status).toBe("failed");
});

it("ends in compensation_failed when a compensator fails", async () => {
  const queue = freshQueue();

  queue.task("reserve", () => 1);
  queue.task("ship", () => {
    throw new Error("nope");
  });
  queue.task("unreserve", () => {
    throw new Error("rollback failed");
  });

  const handle = queue.workflows
    .define("saga-bad-rollback")
    .step("reserve", "reserve", { compensate: "unreserve", maxRetries: 0 })
    .step("ship", "ship", { after: "reserve", maxRetries: 0 })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("compensation_failed");
  expect(nodesByName(handle).reserve?.status).toBe("compensation_failed");
});

it("emits the compensation events in order for a successful rollback", async () => {
  const queue = freshQueue();
  const seen: string[] = [];
  queue.on("workflow.compensating", (event) => seen.push(`compensating:${event.state}`));
  queue.on("workflow.node_compensating", (event) => seen.push(`node_compensating:${event.node}`));
  queue.on("workflow.node_compensated", (event) => seen.push(`node_compensated:${event.node}`));
  queue.on("workflow.compensated", (event) => seen.push(`compensated:${event.state}`));

  queue.task("reserve", () => "reservation");
  queue.task("ship", () => {
    throw new Error("out of stock");
  });
  queue.task("unreserve", () => 1);

  const handle = queue.workflows
    .define("saga-events")
    .step("reserve", "reserve", { compensate: "unreserve" })
    .step("ship", "ship", { after: "reserve", maxRetries: 0 })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("compensated");
  expect(seen).toEqual([
    "compensating:compensating",
    "node_compensating:reserve",
    "node_compensated:reserve",
    "compensated:compensated",
  ]);
});

it("emits node_compensation_failed and compensation_failed when a compensator fails", async () => {
  const queue = freshQueue();
  const seen: string[] = [];
  queue.on("workflow.node_compensation_failed", (event) =>
    seen.push(`node_compensation_failed:${event.node}:${event.error ? "err" : "none"}`),
  );
  queue.on("workflow.compensation_failed", (event) =>
    seen.push(`compensation_failed:${event.error ? "err" : "none"}`),
  );

  queue.task("reserve", () => 1);
  queue.task("ship", () => {
    throw new Error("nope");
  });
  queue.task("unreserve", () => {
    throw new Error("rollback failed");
  });

  const handle = queue.workflows
    .define("saga-events-bad-rollback")
    .step("reserve", "reserve", { compensate: "unreserve", maxRetries: 0 })
    .step("ship", "ship", { after: "reserve", maxRetries: 0 })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("compensation_failed");
  expect(seen).toEqual(["node_compensation_failed:reserve:err", "compensation_failed:err"]);
});

it("fails normally with no compensation when no step declares a compensator", async () => {
  const queue = freshQueue();

  queue.task("reserve", () => 1);
  queue.task("ship", () => {
    throw new Error("nope");
  });

  const handle = queue.workflows
    .define("saga-none")
    .step("reserve", "reserve")
    .step("ship", "ship", { after: "reserve", maxRetries: 0 })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("failed");
  expect(nodesByName(handle).reserve?.status).toBe("completed"); // not rolled back
});
