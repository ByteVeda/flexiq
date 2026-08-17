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
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-cond-")), "queue.db");
  return new Queue({ dbPath });
}

function nodesByName<T extends { nodeName: string }>(handle: {
  nodes: () => T[];
}): Record<string, T | undefined> {
  return Object.fromEntries(handle.nodes().map((n) => [n.nodeName, n]));
}

it("runs an on_failure handler and skips on_success siblings when a step fails", async () => {
  const queue = freshQueue();
  const ran: string[] = [];

  queue.task("risky", () => {
    throw new Error("boom");
  });
  queue.task("recover", () => ran.push("recover"));
  queue.task("celebrate", () => ran.push("celebrate"));

  const handle = queue.workflows
    .define("cond-handler")
    .step("risky", "risky", { maxRetries: 0 })
    .step("recover", "recover", { after: "risky", condition: "on_failure" })
    .step("celebrate", "celebrate", { after: "risky", condition: "on_success" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("failed"); // a node failed, even though it was handled
  const nodes = nodesByName(handle);
  expect(nodes.risky?.status).toBe("failed");
  expect(nodes.recover?.status).toBe("completed");
  expect(nodes.celebrate?.status).toBe("skipped");
  expect(ran).toEqual(["recover"]);
});

it("runs an `always` step regardless of upstream failure", async () => {
  const queue = freshQueue();
  let cleaned = false;

  queue.task("risky", () => {
    throw new Error("boom");
  });
  queue.task("cleanup", () => {
    cleaned = true;
  });

  const handle = queue.workflows
    .define("cond-always")
    .step("risky", "risky", { maxRetries: 0 })
    .step("cleanup", "cleanup", { after: "risky", condition: "always" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  await handle.wait({ timeoutMs: 10_000 });

  expect(cleaned).toBe(true);
  expect(nodesByName(handle).cleanup?.status).toBe("completed");
});

it("propagates a skip to downstream steps", async () => {
  const queue = freshQueue();

  queue.task("risky", () => {
    throw new Error("boom");
  });
  queue.task("noop", () => 1);

  const handle = queue.workflows
    .define("cond-propagate")
    .step("risky", "risky", { maxRetries: 0 })
    .step("mid", "noop", { after: "risky", condition: "on_success" })
    .step("tail", "noop", { after: "mid" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("failed");
  const nodes = nodesByName(handle);
  expect(nodes.mid?.status).toBe("skipped");
  expect(nodes.tail?.status).toBe("skipped"); // skip propagated through `mid`
});

it("runs an on_success step when its predecessor completes", async () => {
  const queue = freshQueue();
  let value = 0;

  queue.task("produce", () => 21);
  queue.task("consume", () => {
    value = 42;
  });

  const handle = queue.workflows
    .define("cond-happy")
    .step("produce", "produce")
    .step("consume", "consume", { after: "produce", condition: "on_success" })
    .submit();

  worker = queue.runWorker({ queues: ["default"] });
  const run = await handle.wait({ timeoutMs: 10_000 });

  expect(run.state).toBe("completed");
  expect(value).toBe(42);
  expect(nodesByName(handle).consume?.status).toBe("completed");
});
