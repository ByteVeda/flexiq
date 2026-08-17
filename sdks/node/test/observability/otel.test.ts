import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  type Attributes,
  type SpanStatus,
  SpanStatusCode,
  type TracerProvider,
  trace,
} from "@opentelemetry/api";
import { afterEach, beforeEach, expect, it } from "vitest";
import { otelMiddleware } from "../../src/contrib/otel";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

interface RecordedSpan {
  name: string;
  attributes: Attributes;
  status?: SpanStatus;
  exceptions: unknown[];
  ended: boolean;
}

/** A minimal recording tracer provider installed globally so the middleware's
 *  `trace.getTracer()` resolves to spans we can inspect. */
function installRecorder(): RecordedSpan[] {
  const spans: RecordedSpan[] = [];
  const provider = {
    getTracer: () => ({
      startSpan(name: string) {
        const span: RecordedSpan = { name, attributes: {}, exceptions: [], ended: false };
        spans.push(span);
        return {
          setAttribute(key: string, value: unknown) {
            span.attributes[key] = value as Attributes[string];
            return this;
          },
          setAttributes(attrs: Attributes) {
            Object.assign(span.attributes, attrs);
            return this;
          },
          setStatus(status: SpanStatus) {
            span.status = status;
            return this;
          },
          recordException(exception: unknown) {
            span.exceptions.push(exception);
          },
          end() {
            span.ended = true;
          },
        };
      },
    }),
  } as unknown as TracerProvider;
  trace.setGlobalTracerProvider(provider);
  return spans;
}

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-otel-")), "q.db") });
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

beforeEach(() => {
  trace.disable();
});

afterEach(() => {
  worker?.stop();
  worker = undefined;
  trace.disable();
});

it("opens an OK span per successful task execution", async () => {
  const spans = installRecorder();
  const queue = newQueue();
  queue.use(otelMiddleware());
  queue.task("add", (a: number, b: number) => a + b);

  queue.enqueue("add", [2, 3]);
  worker = queue.runWorker();

  expect(await waitFor(() => spans.length > 0 && spans[0]?.ended === true)).toBe(true);
  const span = spans[0];
  expect(span?.name).toBe("flexiq.execute.add");
  expect(span?.attributes["flexiq.task_name"]).toBe("add");
  expect(typeof span?.attributes["flexiq.job_id"]).toBe("string");
  expect(span?.status?.code).toBe(SpanStatusCode.OK);
});

it("records the exception and ERROR status when a task throws", async () => {
  const spans = installRecorder();
  const queue = newQueue();
  queue.use(otelMiddleware());
  queue.task(
    "boom",
    () => {
      throw new Error("kaboom");
    },
    { maxRetries: 0 },
  );

  queue.enqueue("boom", []);
  worker = queue.runWorker();

  expect(await waitFor(() => spans.length > 0 && spans[0]?.ended === true)).toBe(true);
  const span = spans[0];
  expect(span?.status?.code).toBe(SpanStatusCode.ERROR);
  expect(span?.status?.message).toBe("kaboom");
  expect(span?.exceptions[0]).toBeInstanceOf(Error);
});

it("honors taskFilter and a custom span name", async () => {
  const spans = installRecorder();
  const queue = newQueue();
  queue.use(
    otelMiddleware({
      taskFilter: (name) => name === "traced",
      spanName: (ctx) => `job:${ctx.taskName}`,
      extraAttributes: () => ({ "app.tier": "batch" }),
    }),
  );
  queue.task("traced", () => 1);
  queue.task("ignored", () => 2);

  queue.enqueue("ignored", []);
  queue.enqueue("traced", []);
  worker = queue.runWorker();

  expect(await waitFor(() => spans.some((s) => s.name === "job:traced"))).toBe(true);
  expect(spans.every((s) => s.name !== "ignored" && !s.name.includes("ignored"))).toBe(true);
  const traced = spans.find((s) => s.name === "job:traced");
  expect(traced?.attributes["app.tier"]).toBe("batch");
});
