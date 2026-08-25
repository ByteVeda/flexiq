import { type JobContext, runInContext } from "./context";
import { middlewareKey } from "./dashboard/stores/middlewareDisables";
import { SerializationError, TaskNotRegisteredError } from "./errors";
import type { Emitter } from "./events";
import type { Middleware, TaskContext } from "./middleware";
import type { JsTaskInvocation, JsTaskOutcome, NativeQueue } from "./native";
import { type ResourceRuntime, runWithResolver } from "./resources";
import { deserializeCall, type PayloadCodec, type Serializer } from "./serializers";
import {
  StepContext,
  StepLatch,
  StepSleepSignal,
  type StepStore,
  stepRetryDecision,
} from "./steps";
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
  /**
   * The middleware chain for a task, after dashboard disables are applied.
   *
   * Takes the job id because an attached executor resolves disables per
   * dispatch — the scheduler attaches the list to the job frame, since the
   * executor has no settings store to read it from. A worker ignores it and
   * reads storage by task name.
   */
  middlewareFor: (taskName: string, jobId: string) => readonly Middleware[];
  emitter: Emitter;
  resources: ResourceRuntime;
  /** Backs progress, published partials, and the cancel-flag poll. */
  queue: NativeQueue;
  /**
   * Overrides how a running job learns it was cancelled.
   *
   * An attached executor reads no storage, so the flag `queue` would poll is
   * never set; its cancels arrive as protocol frames instead, and this reads
   * the native state those land in.
   */
  isCancelled?: (jobId: string) => boolean;
  /**
   * Overrides where a task's progress goes.
   *
   * Same reason as {@link isCancelled}: an executor has no storage, so it
   * sends progress to the scheduler, which applies it.
   */
  setProgress?: (jobId: string, progress: number) => void;
  /** Overrides where a task's log lines and published partials go. */
  writeTaskLog?: (
    jobId: string,
    taskName: string,
    level: string,
    message: string,
    extra?: string,
  ) => void;
  /**
   * Where `ctx.step` commits, when this callback's owner can commit at all.
   *
   * A worker passes its own native handle, so every step is fenced on the id
   * *that* worker claims execution under — two workers on one queue must not
   * share an owner. Absent means nothing here can commit and every step
   * refuses: an attached executor holds no claim and has no channel to commit
   * on, and one started from a process that *does* have storage must refuse
   * too, because the job belongs to the scheduler.
   */
  steps?: StepStore;
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
  const isCancelled = deps.isCancelled ?? ((jobId: string) => queue.isCancelRequested(jobId));
  const setProgress =
    deps.setProgress ??
    ((jobId: string, progress: number) => queue.updateProgress(jobId, progress));
  const writeTaskLog =
    deps.writeTaskLog ??
    ((jobId: string, taskName: string, level: string, message: string, extra?: string) =>
      queue.writeTaskLog(jobId, taskName, level, message, extra));

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
    const chain = middlewareFor(invocation.taskName, invocation.id);

    // Cooperative cancel signal + job context exposed to the handler.
    const controller = new AbortController();
    // One latch per invocation, shared by `ctx.step` and the swallow check
    // below: JavaScript has no error a `catch` misses, so this is the whole of
    // the defence against a body that catches a control signal and returns.
    const latch = new StepLatch();
    const context: JobContext = {
      jobId: invocation.id,
      signal: controller.signal,
      // Step results are encoded with the *queue* serializer, which already
      // carries the queue codec chain — that is how `new Queue({ codec })`
      // encryption reaches `job_steps` with no extra plumbing.
      step: new StepContext(invocation.id, invocation.attempt, serializer, latch, deps.steps),
      setProgress: (progress) => setProgress(invocation.id, progress),
      log: (message, level = "info", extra) =>
        writeTaskLog(
          invocation.id,
          invocation.taskName,
          level,
          message,
          extra === undefined ? undefined : encodeExtra(extra),
        ),
      // A published partial is a task log at level `result`, which is what lets
      // `queue.stream` pick it out of ordinary logs.
      publish: (value) =>
        writeTaskLog(invocation.id, invocation.taskName, "result", "", encodeExtra(value)),
    };
    const poller = setInterval(() => {
      try {
        if (isCancelled(invocation.id)) {
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
      // Before the `after` hooks, which exist to see a result: a body that
      // caught a step control signal and returned did not produce one.
      latch.check();
      for (const mw of chain) {
        await mw.after?.(ctx, result);
      }
      return { result: Buffer.from(serializer.serialize(result)) };
    } catch (error) {
      // A slept attempt is neither a result nor a failure: the sleep row is
      // committed, the claim released and the job already Pending at its
      // deadline. It pairs `before` with `onSleep` rather than `after`, runs
      // no `onError`, and emits `job.sleeping` instead of `job.failed`.
      if (error instanceof StepSleepSignal) {
        warnUnpairedMiddleware(chain);
        for (const mw of chain) {
          try {
            await mw.onSleep?.(ctx, error.wakeAt);
          } catch (hookError) {
            // A sleep is already committed; a hook cannot undo it.
            log.debug(() => `onSleep middleware hook failed for ${invocation.id}`, hookError);
          }
        }
        emitter.emit("job.sleeping", {
          jobId: invocation.id,
          taskName: invocation.taskName,
          queue: invocation.queue,
          wakeAt: error.wakeAt,
          stepKey: error.stepKey,
          durationMs: performance.now() - startedAt,
        });
        return { sleptUntil: error.wakeAt };
      }
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
      // Warns if the job has recorded steps this code no longer runs. Never
      // throws — the side effects already happened.
      context.step.finish();
      try {
        await scope.teardown();
      } catch (error) {
        // dispose errors must not fail an already-settled job
        log.debug(() => `task-scope teardown for ${invocation.id} failed`, error);
      }
    }
  };
}

/**
 * Middleware already warned about for pairing `before` with only `after`.
 *
 * Process-wide and unbounded only in the number of distinct middleware names,
 * which is small and fixed at startup.
 */
const WARNED_UNPAIRED = new Set<string>();

/**
 * Warn once per middleware that opens something in `before` and implements no
 * `onSleep`.
 *
 * A sleep ends the attempt without a result, so such middleware leaks whatever
 * its `before` opened — a span, a scope, a timer. Nothing can be done for it
 * automatically: only the middleware knows how to close what it opened. Naming
 * it once is the honest answer.
 */
function warnUnpairedMiddleware(chain: readonly Middleware[]): void {
  chain.forEach((mw, index) => {
    if (!mw.before || mw.onSleep) {
      return;
    }
    const name = middlewareKey(mw, index);
    if (WARNED_UNPAIRED.has(name)) {
      return;
    }
    WARNED_UNPAIRED.add(name);
    log.warn(
      () =>
        `${name} defines before() but not onSleep(), so whatever its before() opened is ` +
        "left open when an attempt ends in step.sleep. Implement onSleep(ctx, wakeAt) to " +
        "close it.",
    );
  });
}

/**
 * Encode a structured `extra` blob for a task log.
 *
 * Falls back to the value's string form rather than throwing: a circular
 * reference in a log line must not fail the task that wrote it, and a
 * best-effort rendering is more use than a lost entry.
 */
function encodeExtra(value: unknown): string {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

/**
 * Whether this failure may be retried.
 *
 * A step failure answers for itself: the core has already classified it
 * (`classifyStepFailure`), and a divergence or a cap violation will be just as
 * wrong next attempt whatever the task's `retryOn` predicate thinks — that
 * predicate expresses an opinion about the *task's* errors.
 */
export function isRetryable(task: RegisteredTask, error: unknown): boolean {
  const stepDecision = stepRetryDecision(error);
  if (stepDecision !== undefined) {
    return stepDecision;
  }
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
