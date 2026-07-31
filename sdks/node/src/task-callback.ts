import { type JobContext, runInContext } from "./context";
import { SerializationError, TaskNotRegisteredError } from "./errors";
import type { Emitter } from "./events";
import type { Middleware, TaskContext } from "./middleware";
import type { JsTaskInvocation, JsTaskOutcome, NativeQueue } from "./native";
import { type ResourceRuntime, runWithResolver } from "./resources";
import { deserializeCall, type PayloadCodec, type Serializer } from "./serializers";
import { encodeTaskError } from "./task-error";
import type { RegisteredTask } from "./types";
import { createLogger } from "./utils";
import { CACHE_TASK } from "./workflows/cache";

const log = createLogger("task");

/** How often a running job polls the storage cancel flag. */
const CANCEL_POLL_INTERVAL_MS = 200;

/** What running one task needs, independent of how the job arrived. */
export interface TaskCallbackDeps {
  tasks: ReadonlyMap<string, RegisteredTask>;
  serializer: Serializer;
  /** Named codec registry for per-task payload decode (see `TaskOptions.codecs`). */
  codecs?: ReadonlyMap<string, PayloadCodec>;
  /** The middleware chain for a task, after dashboard disables are applied. */
  middlewareFor: (taskName: string) => readonly Middleware[];
  emitter: Emitter;
  resources: ResourceRuntime;
  /** Backs progress, published partials, and the cancel-flag poll. */
  queue: NativeQueue;
}

/**
 * Build the function the native layer calls for each dispatched job.
 *
 * Shared by the in-process worker and the attached executor: a job is a job
 * however it arrived, and duplicating this would mean two places to fix a
 * middleware-ordering or codec bug.
 */
export function createTaskCallback(
  deps: TaskCallbackDeps,
): (invocation: JsTaskInvocation) => Promise<JsTaskOutcome> {
  const { tasks, serializer, codecs, middlewareFor, emitter, resources, queue } = deps;

  return async (invocation: JsTaskInvocation): Promise<JsTaskOutcome> => {
    // Built-in workflow cache-return: echo the single (cached) arg as the result.
    if (invocation.taskName === CACHE_TASK) {
      const [value] = deserializeCall(serializer, invocation.payload);
      return { result: Buffer.from(serializer.serialize(value)) };
    }
    const task = tasks.get(invocation.taskName);
    if (!task) {
      throw new TaskNotRegisteredError(invocation.taskName);
    }
    // Reverse the task's named codecs (see `TaskOptions.codecs`) before decode.
    let payload: Uint8Array = invocation.payload;
    for (const codecName of [...(task.options?.codecs ?? [])].reverse()) {
      const codec = codecs?.get(codecName);
      if (!codec) {
        throw new SerializationError(`no codec registered named "${codecName}"`);
      }
      payload = codec.decode(payload);
    }
    const args = deserializeCall(serializer, payload);
    const ctx: TaskContext = { jobId: invocation.id, taskName: invocation.taskName, args };
    // Resolve the middleware chain BEFORE allocating the cancel poller and
    // task scope — it reads storage and may throw, and nothing would clean
    // those up yet.
    const chain = middlewareFor(invocation.taskName);

    // Cooperative cancel signal + job context exposed to the handler.
    const controller = new AbortController();
    const context: JobContext = {
      jobId: invocation.id,
      signal: controller.signal,
      setProgress: (progress) => queue.updateProgress(invocation.id, progress),
      publish: (value) =>
        queue.writeTaskLog(invocation.id, invocation.taskName, "result", "", JSON.stringify(value)),
    };
    const poller = setInterval(() => {
      try {
        if (queue.isCancelRequested(invocation.id)) {
          controller.abort();
        }
      } catch (error) {
        // transient storage error — retry on the next tick
        log.debug(() => `cancel poll for ${invocation.id} failed`, error);
      }
    }, CANCEL_POLL_INTERVAL_MS);
    poller.unref();

    // Per-invocation resource scope; `useResource`/`inject` resolve against it.
    const scope = resources.createTaskScope();
    const invoke = async (): Promise<unknown> => {
      const inject = task.options?.inject;
      if (inject && inject.length > 0) {
        const deps: Record<string, unknown> = {};
        for (const name of inject) {
          deps[name] = await scope.resolver(name);
        }
        return task.handler(...args, deps);
      }
      return task.handler(...args);
    };

    const startedAt = performance.now();
    try {
      for (const mw of chain) {
        await mw.before?.(ctx);
      }
      const result = await runWithResolver(scope.resolver, () => runInContext(context, invoke));
      for (const mw of chain) {
        await mw.after?.(ctx, result);
      }
      return { result: Buffer.from(serializer.serialize(result)) };
    } catch (error) {
      for (const mw of chain) {
        try {
          await mw.onError?.(ctx, error);
        } catch {
          // onError hooks must not mask the original task failure.
        }
      }
      // Resolve rather than reject: a rejection carries only a string, and the
      // native layer needs the retry verdict alongside the canonical
      // structured-error JSON it stores as the job's error.
      const encoded = encodeTaskError(error);
      // One `job.failed` per failed attempt (the retry/dead verdict follows
      // as its own outcome event once the scheduler settles the job).
      emitter.emit("job.failed", {
        jobId: invocation.id,
        taskName: invocation.taskName,
        error: encoded,
        durationMs: performance.now() - startedAt,
      });
      return { error: encoded, retryable: isRetryable(task, error) };
    } finally {
      clearInterval(poller);
      try {
        await scope.teardown();
      } catch (error) {
        // dispose errors must not fail an already-settled job
        log.debug(() => `task-scope teardown for ${invocation.id} failed`, error);
      }
    }
  };
}

/** Whether a task's `retryOn` predicate accepts this failure. */
export function isRetryable(task: RegisteredTask, error: unknown): boolean {
  const predicate = task.options?.retryOn;
  if (!predicate) {
    return true;
  }
  try {
    return predicate(error);
  } catch (predicateError) {
    log.error(() => "retryOn predicate threw; retrying the failure", predicateError);
    return true;
  }
}
