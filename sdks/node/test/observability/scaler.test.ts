import { mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, expect, it } from "vitest";
import { Queue, serveScaler } from "../../src/index";

let server: Server | undefined;
let base = "";
let queue: Queue;

beforeEach(async () => {
  queue = new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-scaler-")), "q.db") });
  queue.task("work", () => undefined);
  server = serveScaler(queue, { port: 0, host: "127.0.0.1", targetQueueDepth: 5 });
  await new Promise((r) => setTimeout(r, 60));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  server?.close();
  server = undefined;
});

it("reports global queue depth and target for KEDA", async () => {
  queue.enqueue("work");
  queue.enqueue("work");
  const body = (await (await fetch(`${base}/api/scaler`)).json()) as {
    metricValue: number;
    targetValue: number;
    queueName: string;
  };
  expect(body.metricValue).toBe(2);
  expect(body.targetValue).toBe(5);
  expect(body.queueName).toBe("*");
});

it("filters the metric to a queue via ?queue=", async () => {
  queue.enqueue("work", undefined, { queue: "emails" });
  const all = (await (await fetch(`${base}/api/scaler`)).json()) as { metricValue: number };
  expect(all.metricValue).toBe(1);
  const emails = (await (await fetch(`${base}/api/scaler?queue=emails`)).json()) as {
    metricValue: number;
    queueName: string;
  };
  expect(emails.metricValue).toBe(1);
  expect(emails.queueName).toBe("emails");
  const other = (await (await fetch(`${base}/api/scaler?queue=other`)).json()) as {
    metricValue: number;
  };
  expect(other.metricValue).toBe(0);
});

it("serves a health check", async () => {
  const res = await fetch(`${base}/health`);
  expect(res.status).toBe(200);
  expect(await res.json()).toEqual({ status: "ok" });
});

it("404s an unknown path", async () => {
  expect((await fetch(`${base}/nope`)).status).toBe(404);
});
