import { once } from "node:events";
import { mkdtempSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { type EventSinksConfig, Queue, type Worker } from "../../src/index";

const SINK = "receiver";
const STARTED = "org.byteveda.flexiq.job.started";
const COMPLETED = "org.byteveda.flexiq.job.completed";

type CloudEvent = Record<string, unknown> & { data?: Record<string, unknown> };

let receiver: Server | undefined;
let received: CloudEvent[] = [];
let sinkUrl = "";
let worker: Worker | undefined;
/** How long the receiver holds each response, to keep events in flight. */
let replyDelayMs = 0;

beforeEach(async () => {
  received = [];
  replyDelayMs = 0;
  receiver = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as CloudEvent | CloudEvent[];
      // A batching sink posts an array; one event per request otherwise.
      received.push(...(Array.isArray(body) ? body : [body]));
      setTimeout(() => res.writeHead(200).end(), replyDelayMs);
    });
  });
  receiver.listen(0, "127.0.0.1");
  await once(receiver, "listening");
  sinkUrl = `http://127.0.0.1:${(receiver.address() as AddressInfo).port}/events`;
});

afterEach(async () => {
  await worker?.stop();
  worker = undefined;
  receiver?.close();
  receiver = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-node-events-")), "q.db") });
}

function httpSinks(url: string): EventSinksConfig {
  return {
    sinks: [{ kind: "http", name: SINK, url, allow: ["127.0.0.1"], allow_loopback: true }],
  };
}

it("throws on a document naming no sinks", () => {
  const queue = newQueue();
  expect(() => queue.runWorker({ eventSinks: { sinks: [] } })).toThrow(/names no sinks/);
});

it("throws on malformed JSON text", () => {
  const queue = newQueue();
  expect(() => queue.runWorker({ eventSinks: "{not json" })).toThrow(/events config is not valid/);
});

it("throws on an http sink with an empty allow list", () => {
  const queue = newQueue();
  const config: EventSinksConfig = {
    sinks: [{ kind: "http", name: SINK, url: sinkUrl, allow: [], allow_loopback: true }],
  };
  expect(() => queue.runWorker({ eventSinks: config })).toThrow(
    /events sink 'receiver': allow must name at least one host/,
  );
});

it.each([
  -1,
  1.5,
  Number.NaN,
  Number.POSITIVE_INFINITY,
])("throws on eventSinksDrainMs %s", (drain) => {
  const queue = newQueue();
  expect(() =>
    queue.runWorker({ eventSinks: httpSinks(sinkUrl), eventSinksDrainMs: drain }),
  ).toThrow(RangeError);
});

it("reports no stats when started without sinks", () => {
  const queue = newQueue();
  worker = queue.runWorker();
  expect(worker.eventSinkStats()).toEqual([]);
});

it("sends job.started and job.completed to an http sink", async () => {
  const queue = newQueue();
  queue.task("echo", (value: string) => value);
  worker = queue.runWorker({ eventSinks: JSON.stringify(httpSinks(sinkUrl)) });
  const running = worker;

  const id = queue.enqueue("echo", ["hello"]);
  expect(await queue.result(id, { timeoutMs: 10_000 })).toBe("hello");

  const jobEvents = (): Map<unknown, CloudEvent> =>
    new Map(received.filter((event) => event.subject === id).map((event) => [event.type, event]));
  await vi.waitFor(
    () => {
      expect(jobEvents().has(STARTED)).toBe(true);
      expect(jobEvents().has(COMPLETED)).toBe(true);
    },
    { timeout: 10_000, interval: 25 },
  );
  for (const type of [STARTED, COMPLETED]) {
    const event = jobEvents().get(type);
    expect(event?.flexiqqueue).toBe("default");
    expect(event?.flexiqtask).toBe("echo");
    expect(event?.data?.job_id).toBe(id);
    // No sink set `include_payload`, so no payload leaves the process.
    expect(event?.data).not.toHaveProperty("payload_base64");
  }

  // The counter moves once the receiver's reply is read, just after the post.
  await vi.waitFor(
    () => {
      expect(running.eventSinkStats()[0]?.delivered ?? 0).toBeGreaterThanOrEqual(2);
    },
    { timeout: 10_000, interval: 25 },
  );
  const [stats] = running.eventSinkStats();
  expect(stats?.name).toBe(SINK);
  expect(stats?.kind).toBe("http");
  expect(stats?.droppedRejected).toBe(0);
  expect(stats?.droppedFailed).toBe(0);
});

it("resolves stop only after a slow sink has received the job's events", async () => {
  replyDelayMs = 300;
  const queue = newQueue();
  queue.task("echo", (value: string) => value);
  const running = queue.runWorker({ eventSinks: httpSinks(sinkUrl) });
  worker = running;

  const id = queue.enqueue("echo", ["hello"]);
  expect(await queue.result(id, { timeoutMs: 10_000 })).toBe("hello");
  // Each reply is held, so both events are still in flight when stop starts.
  await running.stop();

  const [stats] = running.eventSinkStats();
  expect(stats?.delivered).toBe(2);
  expect(stats?.queued).toBe(0);
  expect(stats?.droppedShutdown).toBe(0);
});

it("bounds stop by the drain budget when a job never settles", async () => {
  const queue = newQueue();
  let entered = false;
  queue.task("hang", () => {
    entered = true;
    return new Promise<never>(() => {});
  });
  const running = queue.runWorker({ eventSinks: httpSinks(sinkUrl), eventSinksDrainMs: 200 });
  worker = running;

  queue.enqueue("hang", []);
  await vi.waitFor(() => expect(entered).toBe(true), { timeout: 10_000, interval: 25 });
  const startedAt = Date.now();
  await running.stop();

  // The hung job holds the result loop open; stop must not wait on it.
  expect(Date.now() - startedAt).toBeLessThan(3_000);
  expect(running.eventSinkStats()[0]?.queued).toBe(0);
});
