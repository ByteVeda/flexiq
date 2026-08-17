import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker, type WorkerEvent } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-wlc-")), "q.db") });
}

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("emits worker.started, worker.online, then worker.stopped in order", async () => {
  const queue = newQueue();
  const seen: Array<{ event: string; workerId: string }> = [];
  queue.on("worker.started", (event) => seen.push({ event: "started", workerId: event.workerId }));
  queue.on("worker.online", (event) => seen.push({ event: "online", workerId: event.workerId }));
  queue.on("worker.stopped", (event) => seen.push({ event: "stopped", workerId: event.workerId }));
  queue.task("noop", () => 1);

  worker = queue.runWorker({ queues: ["default"], heartbeatIntervalMs: 50 });
  expect(await waitFor(() => seen.some((entry) => entry.event === "online"))).toBe(true);
  worker.stop();
  worker = undefined;

  expect(seen.map((entry) => entry.event)).toEqual(["started", "online", "stopped"]);
  expect(new Set(seen.map((entry) => entry.workerId)).size).toBe(1);
});

it("reports the configured queues on worker.started", () => {
  const queue = newQueue();
  const started: WorkerEvent[] = [];
  queue.on("worker.started", (event) => started.push(event));

  worker = queue.runWorker({ queues: ["emails", "reports"] });

  expect(started).toHaveLength(1);
  expect(started[0]?.queues).toEqual(["emails", "reports"]);
});

it("emits no worker.offline while the reap list stays empty", async () => {
  const queue = newQueue();
  const offline: WorkerEvent[] = [];
  const online: WorkerEvent[] = [];
  queue.on("worker.offline", (event) => offline.push(event));
  queue.on("worker.online", (event) => online.push(event));

  worker = queue.runWorker({ heartbeatIntervalMs: 25 });

  // Several heartbeats pass; a lone live worker reaps nobody.
  expect(await waitFor(() => online.length === 1)).toBe(true);
  await new Promise((resolve) => setTimeout(resolve, 150));
  expect(offline).toEqual([]);
  expect(online).toHaveLength(1); // online reported exactly once
});
