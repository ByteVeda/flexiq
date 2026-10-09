// A workflow's steps join the trace of whoever submitted it. The submit's
// context is stored on the run, and every node job — pre-enqueued, fanned out,
// released later by the tracker, or compensating — carries it, so each step's
// execute span is a child of the submit span (flat, not chained).
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { context, propagation, type SpanContext } from "@opentelemetry/api";
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

const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

const exporter = new InMemorySpanExporter();
const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
const tracer = provider.getTracer("test");
let worker: Worker | undefined;

beforeAll(() => {
  // Another file in the same worker may have registered these globals.
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

afterEach(async () => {
  // Teardown must finish before the next test resets the exporter.
  await worker?.stop();
  worker = undefined;
});

function newQueue(traced = true): Queue {
  const queue = new Queue({
    dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-workflow-trace-")), "q.db"),
  });
  if (traced) {
    queue.use(otelMiddleware({ tracerProvider: provider }));
  }
  return queue;
}

/** Run `fn` under an active `submit` span, returning both. */
function inSubmit<T>(fn: () => T): { value: T; submit: SpanContext } {
  return tracer.startActiveSpan("submit", (span) => {
    try {
      return { value: fn(), submit: span.spanContext() };
    } finally {
      span.end();
    }
  });
}

function executions(task: string): ReadableSpan[] {
  return exporter.getFinishedSpans().filter((span) => span.name === `flexiq.execute.${task}`);
}

function expectChildrenOf(spans: ReadableSpan[], submit: SpanContext): void {
  expect(spans.length).toBeGreaterThan(0);
  for (const span of spans) {
    expect(span.spanContext().traceId).toBe(submit.traceId);
    expect(span.parentSpanContext?.spanId).toBe(submit.spanId);
  }
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

function nodeMetadata(queue: Queue, nodes: { nodeName: string; jobId?: string | null }[]) {
  return Object.fromEntries(
    nodes.map((node) => [
      node.nodeName,
      JSON.parse(queue.getJob(node.jobId ?? "")?.metadata ?? "{}"),
    ]),
  );
}

it("makes every step a child of the submit span", async () => {
  const queue = newQueue();
  queue.task("first", () => 1);
  queue.task("second", () => 2);

  const { value: handle, submit } = inSubmit(() =>
    queue.workflows
      .define("traced-linear")
      .step("a", "first")
      .step("b", "second", { after: "a" })
      .submit(),
  );
  worker = queue.runWorker({ queues: ["default"] });

  expect((await handle.wait({ timeoutMs: 10_000 })).state).toBe("completed");
  await waitFor(() => executions("second").length === 1);
  expectChildrenOf([...executions("first"), ...executions("second")], submit);
});

it("carries the trace into fan-out children and the released fan-in", async () => {
  const queue = newQueue();
  queue.task("source", () => [1, 2]);
  queue.task("double", (n: number) => n * 2);
  queue.task("sum", (results: number[]) => results.reduce((acc, x) => acc + x, 0));

  const { value: handle, submit } = inSubmit(() =>
    queue.workflows
      .define("traced-fanout")
      .step("source", "source")
      .fanOut("process", { after: "source", task: "double", itemsFrom: "source" })
      .fanIn("collect", { after: "process", task: "sum" })
      .submit(),
  );
  worker = queue.runWorker({ queues: ["default"] });

  expect((await handle.wait({ timeoutMs: 10_000 })).state).toBe("completed");
  await waitFor(() => executions("sum").length === 1);
  expect(executions("double")).toHaveLength(2);
  expectChildrenOf([...executions("double"), ...executions("sum")], submit);
});

it("carries the trace into a compensation", async () => {
  const queue = newQueue();
  queue.task("reserve", () => "reservation");
  queue.task("ship", () => {
    throw new Error("out of stock");
  });
  queue.task("unreserve", () => undefined);

  const { value: handle, submit } = inSubmit(() =>
    queue.workflows
      .define("traced-saga")
      .step("reserve", "reserve", { compensate: "unreserve" })
      .step("ship", "ship", { after: "reserve", maxRetries: 0 })
      .submit(),
  );
  worker = queue.runWorker({ queues: ["default"] });

  expect((await handle.wait({ timeoutMs: 10_000 })).state).toBe("compensated");
  await waitFor(() => executions("unreserve").length === 1);
  expectChildrenOf(executions("unreserve"), submit);
});

it("stamps an explicit trace context beside the routing keys", () => {
  const queue = newQueue(false);
  queue.task("noop", () => undefined);
  const builder = queue.workflows
    .define("explicit-trace")
    .step("a", "noop")
    .step("b", "noop", { after: "a" });

  const handle = queue.workflows.submit(builder, {
    traceContext: { traceparent: TRACEPARENT, tracestate: "vendor=a" },
  });

  for (const [name, metadata] of Object.entries(nodeMetadata(queue, handle.nodes()))) {
    expect(metadata).toEqual({
      workflow_run_id: handle.runId,
      workflow_node_name: name,
      traceparent: TRACEPARENT,
      tracestate: "vendor=a",
    });
  }
});

it("lets an explicit trace context win over the middleware", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  const builder = queue.workflows.define("explicit-wins").step("a", "noop");

  const { value: handle } = inSubmit(() =>
    queue.workflows.submit(builder, { traceContext: { traceparent: TRACEPARENT } }),
  );

  expect(nodeMetadata(queue, handle.nodes()).a?.traceparent).toBe(TRACEPARENT);
});

it("submits the run even when an onWorkflowSubmit hook throws", () => {
  const queue = newQueue(false);
  queue.use({
    onWorkflowSubmit() {
      throw new Error("hook failed");
    },
  });
  queue.task("noop", () => undefined);
  const builder = queue.workflows.define("hook-throws").step("a", "noop");

  const handle = queue.workflows.submit(builder, { traceContext: { traceparent: TRACEPARENT } });

  expect(nodeMetadata(queue, handle.nodes()).a?.traceparent).toBe(TRACEPARENT);
});

it("carries only the routing keys outside any span", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const handle = queue.workflows.define("untraced").step("a", "noop").submit();

  expect(Object.keys(nodeMetadata(queue, handle.nodes()).a ?? {}).sort()).toEqual([
    "workflow_node_name",
    "workflow_run_id",
  ]);
});
