// Trace context crosses the queue: enqueue injects, execution continues. Real
// OpenTelemetry SDK spans rather than a recorder — parentage is the property
// under test, and a recorder cannot get it wrong.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { context, propagation, type SpanContext, SpanKind } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  type ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from "vitest";
import { otelMiddleware } from "../../src/contrib/otel";
import { Queue, type Worker } from "../../src/index";

const exporter = new InMemorySpanExporter();
const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
const tracer = provider.getTracer("test");
let worker: Worker | undefined;

beforeAll(() => {
  // A worker process outlives a test file, and the API refuses to replace a
  // global another file registered (Sentry's SDK registers both), so clear first.
  context.disable();
  propagation.disable();
  expect(context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())).toBe(
    true,
  );
  expect(propagation.setGlobalPropagator(new W3CTraceContextPropagator())).toBe(true);
});

afterAll(() => {
  context.disable();
  propagation.disable();
});

beforeEach(() => {
  exporter.reset();
});

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  const queue = new Queue({
    dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-otel-propagation-")), "q.db"),
  });
  queue.use(otelMiddleware({ tracerProvider: provider }));
  return queue;
}

/** Run `fn` under an active `request` span, returning both. */
function inRequest<T>(fn: () => T): { value: T; request: SpanContext } {
  return tracer.startActiveSpan("request", (span) => {
    try {
      return { value: fn(), request: span.spanContext() };
    } finally {
      span.end();
    }
  });
}

function traceparentOf({ traceId, spanId, traceFlags }: SpanContext): string {
  return `00-${traceId}-${spanId}-${traceFlags.toString(16).padStart(2, "0")}`;
}

function executions(task: string): ReadableSpan[] {
  return exporter.getFinishedSpans().filter((span) => span.name === `flexiq.execute.${task}`);
}

async function waitFor(predicate: () => boolean, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error("timed out waiting for spans");
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

it("injects the caller's span into the job's metadata", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const { value: id, request } = inRequest(() => queue.enqueue("noop", []));

  const metadata = JSON.parse(queue.getJob(id)?.metadata ?? "{}");
  expect(metadata.traceparent).toBe(traceparentOf(request));
});

it("keeps user metadata and leaves an unparseable one alone", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const { value: ids } = inRequest(() => [
    queue.enqueue("noop", [], { metadata: '{"tenant":"acme"}' }),
    queue.enqueue("noop", [], { metadata: "not json" }),
  ]);

  const merged = JSON.parse(queue.getJob(ids[0] ?? "")?.metadata ?? "{}");
  expect(merged.tenant).toBe("acme");
  expect(typeof merged.traceparent).toBe("string");
  expect(queue.getJob(ids[1] ?? "")?.metadata).toBe("not json");
});

it("injects nothing outside any span", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const id = queue.enqueue("noop", []);

  expect(queue.getJob(id)?.metadata ?? null).toBeNull();
});

it("makes the execute span a child of the enqueuer", async () => {
  const queue = newQueue();
  queue.task("add", (a: number, b: number) => a + b);

  const { request } = inRequest(() => queue.enqueue("add", [1, 2]));
  worker = queue.runWorker();

  await waitFor(() => executions("add").length === 1);
  const [execute] = executions("add");
  expect(execute?.kind).toBe(SpanKind.CONSUMER);
  expect(execute?.spanContext().traceId).toBe(request.traceId);
  expect(execute?.parentSpanContext?.spanId).toBe(request.spanId);
});

it("continues the trace for a job enqueued inside a task", async () => {
  const queue = newQueue();
  queue.task("child", () => "done");
  queue.task("parent", () => queue.enqueue("child", []));

  const { request } = inRequest(() => queue.enqueue("parent", []));
  worker = queue.runWorker();

  await waitFor(() => executions("child").length === 1);
  const [outer] = executions("parent");
  const [inner] = executions("child");
  expect(inner?.spanContext().traceId).toBe(request.traceId);
  expect(inner?.parentSpanContext?.spanId).toBe(outer?.spanContext().spanId);
});

it("parents every attempt of a retried job to the enqueuer", async () => {
  const queue = newQueue();
  let attempts = 0;
  queue.task(
    "flaky",
    () => {
      attempts += 1;
      if (attempts === 1) {
        throw new Error("first attempt fails");
      }
      return "ok";
    },
    { maxRetries: 1, retryBackoff: { baseMs: 10, maxMs: 20 } },
  );

  const { request } = inRequest(() => queue.enqueue("flaky", []));
  worker = queue.runWorker();

  await waitFor(() => executions("flaky").length === 2);
  const parents = new Set(executions("flaky").map((span) => span.parentSpanContext?.spanId));
  expect([...parents]).toEqual([request.spanId]);
});
