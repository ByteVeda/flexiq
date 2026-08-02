import { AsyncLocalStorage } from "node:async_hooks";
import type { TaskLogLevel } from "./types";

/**
 * Ambient context available to a running task via {@link currentJob}. Mirrors
 * the Python shell's `current_job`.
 */
export interface JobContext {
  /** The running job's id. */
  readonly jobId: string;
  /** Aborts when cancellation is requested — check `signal.aborted` or listen. */
  readonly signal: AbortSignal;
  /** Report progress (0–100) for observability. */
  setProgress(progress: number): void;
  /**
   * Write a structured log line against this job, visible in the dashboard and
   * through `queue.taskLogs(jobId)`.
   *
   * `extra` must be JSON-serializable; a value that is not is stored as its
   * string form rather than failing the task.
   */
  log(message: string, level?: TaskLogWriteLevel, extra?: unknown): void;
  /**
   * Publish a partial result, consumable live via `queue.stream(jobId)`. The
   * value must be JSON-serializable. Use to stream progress from a long-running
   * task (ETL, ML steps, batch processing).
   */
  publish(value: unknown): void;
}

/**
 * Levels {@link JobContext.log} accepts.
 *
 * `result` is excluded: a published partial is a task log at that level, and
 * {@link JobContext.publish} is how one is written. Letting `log` forge one
 * would put an unencoded message where `queue.stream` expects a payload.
 */
export type TaskLogWriteLevel = Exclude<TaskLogLevel, "result">;

const store = new AsyncLocalStorage<JobContext>();

/** The context of the task running on this async stack, or `undefined`. */
export function currentJob(): JobContext | undefined {
  return store.getStore();
}

/** Run `fn` with `context` bound as the ambient job context. @internal */
export function runInContext<T>(context: JobContext, fn: () => T): T {
  return store.run(context, fn);
}
