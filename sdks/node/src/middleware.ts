import type { OutcomeEvent } from "./events";
import type { EnqueueOptions } from "./types";

/** Context for a task as it executes on a worker. */
export interface TaskContext {
  jobId: string;
  taskName: string;
  args: unknown[];
}

/**
 * Context for a job being enqueued, passed to {@link Middleware.onEnqueue} before
 * serialization. Mutate `args`/`options` in place to validate, redact, or rewrite
 * the job; throw to abort the enqueue.
 */
export interface EnqueueContext {
  readonly taskName: string;
  /** Positional args, mutable before they are serialized. */
  args: unknown[];
  /** Enqueue options, mutable before they reach the core. */
  options: EnqueueOptions;
}

/**
 * Cross-cutting hooks around task execution and job outcomes. Register with
 * {@link Queue.use}. `onEnqueue` runs (sync) on the enqueuing side before
 * serialization; `before`/`after`/`onError`/`onSleep` wrap execution (awaited,
 * counted toward the timeout); the outcome hooks fire after the core decides
 * the result.
 */
export interface Middleware {
  /**
   * Stable name used by the dashboard's per-task middleware toggles.
   * Defaults to the class name for class-based middleware.
   */
  name?: string;
  onEnqueue?(ctx: EnqueueContext): void;
  before?(ctx: TaskContext): void | Promise<void>;
  after?(ctx: TaskContext, result: unknown): void | Promise<void>;
  /**
   * Called when an attempt ends in a `ctx.step.sleep`. Pairs with `before`.
   *
   * Every `before` is matched by exactly one of `after` or `onSleep`. A sleep
   * is not a result: `after(ctx, undefined)` is indistinguishable from "the
   * task returned undefined", which would close a tracing span as a success and
   * increment a success counter — both wrong for an attempt that has not
   * finished. Middleware that opens something in `before` and implements only
   * `after` leaks it on a sleep, and the runner warns once about it.
   *
   * @param wakeAt Deadline the job was rescheduled to, in Unix milliseconds.
   */
  onSleep?(ctx: TaskContext, wakeAt: number): void | Promise<void>;
  onError?(ctx: TaskContext, error: unknown): void | Promise<void>;
  onCompleted?(event: OutcomeEvent): void;
  onRetry?(event: OutcomeEvent): void;
  onDeadLetter?(event: OutcomeEvent): void;
  onCancel?(event: OutcomeEvent): void;
}
