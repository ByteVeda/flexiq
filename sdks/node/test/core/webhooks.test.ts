import { createHmac } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, WebhookValidationError, type Worker } from "../../src/index";

// These deliveries target a loopback receiver, which the SSRF guard blocks by default.
process.env.TASKITO_WEBHOOKS_ALLOW_PRIVATE = "1";

let worker: Worker | undefined;
let target: Server | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
  target?.close();
  target = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "taskito-wh-")), "q.db") });
}

interface Received {
  body: string;
  signature?: string;
}

function startTarget(received: Received[]): Promise<number> {
  return new Promise((resolve) => {
    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      let body = "";
      req.on("data", (chunk) => {
        body += chunk;
      });
      req.on("end", () => {
        const signature = req.headers["x-taskito-signature"];
        received.push({ body, signature: typeof signature === "string" ? signature : undefined });
        res.writeHead(200).end();
      });
    });
    target = server;
    server.listen(0, () => resolve((server.address() as AddressInfo).port));
  });
}

it("delivers a signed webhook when a job completes", async () => {
  const received: Received[] = [];
  const port = await startTarget(received);
  const queue = newQueue();
  const secret = "s3cr3t";
  queue.webhooks.create({
    url: `http://127.0.0.1:${port}/hook`,
    events: ["job.completed"],
    secret,
  });
  queue.task("add", (a: number, b: number) => a + b);
  queue.enqueue("add", [2, 3]);
  worker = queue.runWorker();

  for (let i = 0; i < 100 && received.length === 0; i++) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const first = received[0];
  expect(first).toBeDefined();
  if (!first) {
    return;
  }
  const payload = JSON.parse(first.body) as { event: string; taskName: string };
  expect(payload.event).toBe("job.completed");
  expect(payload.taskName).toBe("add");
  const expected = `sha256=${createHmac("sha256", secret).update(first.body).digest("hex")}`;
  expect(first.signature).toBe(expected);
});

it("delivers job.enqueued with no worker running", async () => {
  const received: Received[] = [];
  const port = await startTarget(received);
  const queue = newQueue();
  queue.webhooks.create({ url: `http://127.0.0.1:${port}/hook`, events: ["job.enqueued"] });
  queue.task("add", (a: number, b: number) => a + b);

  const jobId = queue.enqueue("add", [2, 3]);

  for (let i = 0; i < 100 && received.length === 0; i++) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const first = received[0];
  expect(first).toBeDefined();
  const payload = JSON.parse(first?.body ?? "{}") as Record<string, unknown>;
  expect(payload.event).toBe("job.enqueued");
  expect(payload.jobId).toBe(jobId);
  expect(payload.taskName).toBe("add");
  expect(payload.queue).toBe("default");
});

it("delivers queue.paused without job identity fields", async () => {
  const received: Received[] = [];
  const port = await startTarget(received);
  const queue = newQueue();
  const hook = queue.webhooks.create({
    url: `http://127.0.0.1:${port}/hook`,
    events: ["queue.paused"],
  });

  queue.pauseQueue("emails");

  for (let i = 0; i < 100 && received.length === 0; i++) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const payload = JSON.parse(received[0]?.body ?? "{}") as Record<string, unknown>;
  expect(payload).toEqual({ event: "queue.paused", queue: "emails" });

  // The delivery record carries no job identity either.
  for (let i = 0; i < 100 && queue.webhooks.deliveryCount(hook.id) === 0; i++) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const delivery = queue.webhooks.deliveries(hook.id)[0];
  expect(delivery?.taskName).toBeNull();
  expect(delivery?.jobId).toBeNull();
});

it("still delivers task-less events to a task-filtered webhook", async () => {
  const received: Received[] = [];
  const port = await startTarget(received);
  const queue = newQueue();
  queue.webhooks.create({
    url: `http://127.0.0.1:${port}/hook`,
    events: [],
    taskFilter: ["only_this"],
  });
  queue.task("other", () => 1);

  queue.enqueue("other"); // job.enqueued for a filtered-out task: dropped
  queue.pauseQueue("emails"); // no task name: passes the filter

  for (let i = 0; i < 100 && received.length === 0; i++) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const events = received.map((r) => (JSON.parse(r.body) as { event: string }).event);
  expect(events).toContain("queue.paused");
  expect(events).not.toContain("job.enqueued");
});

it("exposes all 29 subscribable event names", () => {
  const queue = newQueue();
  const names = queue.webhooks.eventTypes();
  expect(names).toHaveLength(29);
  expect(names).toContain("job.enqueued");
  expect(names).toContain("worker.online");
  expect(names).toContain("workflow.submitted");
  expect(names).toContain("predicate.rejected");
  expect(names).toContain("predicate.skipped");
  expect(names).toContain("predicate.deferred");
  // Reserved here, but subscribable so a cross-SDK webhook config still loads.
  expect(names).toContain("predicate.cancelled");
});

it("creates, lists, and deletes webhooks", () => {
  const queue = newQueue();
  const hook = queue.webhooks.create({ url: "http://example.com/h", events: [] });
  expect(queue.webhooks.list().map((w) => w.id)).toContain(hook.id);
  expect(queue.webhooks.delete(hook.id)).toBe(true);
  expect(queue.webhooks.list().map((w) => w.id)).not.toContain(hook.id);
});

it("rejects malformed webhook definitions before persisting", () => {
  const queue = newQueue();
  expect(() => queue.webhooks.create({ url: "", events: [] })).toThrow(WebhookValidationError);
  expect(() => queue.webhooks.create({ url: "not-a-url", events: [] })).toThrow(/not a valid URL/);
  expect(() =>
    queue.webhooks.create({ url: "https://x.test", events: [], maxRetries: -1 }),
  ).toThrow(/maxRetries/);
  expect(() => queue.webhooks.create({ url: "https://x.test", events: [], timeoutMs: 0 })).toThrow(
    /timeout/,
  );
  expect(queue.webhooks.list()).toHaveLength(0);
});
