// OpenTelemetry tracing for FlexiQ task execution. Optional integration —
// import from `flexiq/contrib/otel`; requires `@opentelemetry/api` as a peer.
//
// Register with `queue.use(otelMiddleware())`. Each execution attempt becomes one
// span (`flexiq.execute.<task>`); a retry is a fresh attempt and thus a new span.
//
// Trace context crosses the queue: an enqueue injects the active context into
// the job's metadata (`traceparent`/`tracestate`, merged, never replacing the
// caller's keys) through the registered propagator, and the execute span is a
// child of it — whichever SDK enqueued the job. An enqueue made inside a task
// with no active span of its own continues that task's execute span. A workflow
// submit stores the same context on the run, so every step's execute span is a
// child of the submit.

import {
  type Attributes,
  type Context,
  context,
  isSpanContextValid,
  propagation,
  type Span,
  SpanKind,
  SpanStatusCode,
  type TracerProvider,
  trace,
} from "@opentelemetry/api";
import { currentJob } from "../context";
import type { Middleware, TaskContext } from "../middleware";
import { mergeTraceCarrier, traceCarrier } from "../trace-context";

/** Options for {@link otelMiddleware}. */
export interface OtelMiddlewareOptions {
  /** Tracer name passed to `trace.getTracer` (default `"flexiq"`). */
  tracerName?: string;
  /** Prefix for span attribute keys (default `"flexiq"`). */
  attributePrefix?: string;
  /** Override the span name (default `"<prefix>.execute.<taskName>"`). */
  spanName?: (ctx: TaskContext) => string;
  /** Extra attributes merged onto the span at start. */
  extraAttributes?: (ctx: TaskContext) => Attributes;
  /** Only trace tasks for which this returns true (default: all). */
  taskFilter?: (taskName: string) => boolean;
  /** Provider to take the tracer from (default: the globally registered one). */
  tracerProvider?: TracerProvider;
}

/**
 * Build {@link Middleware} that wraps each task execution in an OpenTelemetry span.
 * The span starts in `before`, ends `OK` in `after`, ends `ERROR` (recording the
 * exception) in `onError`, and ends with no status in `onSleep` — an attempt
 * that slept is neither.
 */
export function otelMiddleware(options: OtelMiddlewareOptions = {}): Middleware {
  const tracerName = options.tracerName ?? "flexiq";
  const prefix = options.attributePrefix ?? "flexiq";
  const tracer = (options.tracerProvider ?? trace.getTracerProvider()).getTracer(tracerName);
  const spans = new Map<string, Span>();

  const tracked = (taskName: string): boolean => options.taskFilter?.(taskName) ?? true;

  // The caller's own span if it has one, else the execute span of the task the
  // enqueue runs inside: hooks cannot wrap the handler, so that span is never
  // active there unless the caller made it so.
  const enqueueContext = (): Context => {
    const active = context.active();
    const own = trace.getSpanContext(active);
    if (own && isSpanContextValid(own)) {
      return active;
    }
    const job = currentJob();
    const running = job ? spans.get(job.jobId) : undefined;
    return running ? trace.setSpan(active, running) : active;
  };

  return {
    onEnqueue(ctx) {
      if (!tracked(ctx.taskName)) {
        return;
      }
      const carrier: Record<string, string> = {};
      propagation.inject(enqueueContext(), carrier);
      const merged = mergeTraceCarrier(ctx.options.metadata ?? undefined, carrier);
      if (merged !== undefined) {
        ctx.options.metadata = merged;
      }
    },

    onWorkflowSubmit(ctx) {
      // A carrier the caller passed explicitly wins, as metadata keys do.
      if (ctx.traceContext && Object.keys(ctx.traceContext).length > 0) {
        return;
      }
      const carrier: Record<string, string> = {};
      propagation.inject(enqueueContext(), carrier);
      if (Object.keys(carrier).length > 0) {
        ctx.traceContext = carrier;
      }
    },

    before(ctx) {
      if (!tracked(ctx.taskName)) {
        return;
      }
      const name = options.spanName?.(ctx) ?? `${prefix}.execute.${ctx.taskName}`;
      const parent = propagation.extract(context.active(), traceCarrier(ctx.metadata));
      const span = tracer.startSpan(name, { kind: SpanKind.CONSUMER }, parent);
      span.setAttribute(`${prefix}.job_id`, ctx.jobId);
      span.setAttribute(`${prefix}.task_name`, ctx.taskName);
      const extra = options.extraAttributes?.(ctx);
      if (extra) {
        span.setAttributes(extra);
      }
      spans.set(ctx.jobId, span);
    },

    after(ctx) {
      const span = spans.get(ctx.jobId);
      if (!span) {
        return;
      }
      span.setStatus({ code: SpanStatusCode.OK });
      span.end();
      spans.delete(ctx.jobId);
    },

    onError(ctx, error) {
      const span = spans.get(ctx.jobId);
      if (!span) {
        return;
      }
      const message = error instanceof Error ? error.message : String(error);
      span.recordException(error instanceof Error ? error : message);
      span.setStatus({ code: SpanStatusCode.ERROR, message });
      span.end();
      spans.delete(ctx.jobId);
    },

    // End the span for an attempt that slept, without calling it a result. The
    // span has to end — the attempt is over and the worker slot is gone — but
    // its status stays unset: the task neither succeeded nor failed, and
    // marking it OK would make a job that sleeps three times look like three
    // successful executions.
    onSleep(ctx, wakeAt) {
      const span = spans.get(ctx.jobId);
      if (!span) {
        return;
      }
      span.setAttribute(`${prefix}.slept`, true);
      span.addEvent("sleep", { [`${prefix}.wake_at`]: wakeAt });
      span.end();
      spans.delete(ctx.jobId);
    },
  };
}
