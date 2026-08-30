import {
  applyQueueOverrides,
  applyTaskOverrides,
  MiddlewareDisableStore,
  middlewareKey,
  OverridesStore,
} from "./dashboard/stores";
import { type Emitter, OUTCOME_KIND_EVENTS, type OutcomeEvent } from "./events";
import type { Middleware } from "./middleware";
import type {
  JsOutcome,
  NativeQueue,
  NativeWorker,
  WorkerOptions as NativeWorkerOptions,
  QueueConfigInput,
  TaskConfigInput,
} from "./native";
import type { ResourceRuntime } from "./resources";
import { deserializeCall, type PayloadCodec, type Serializer } from "./serializers";
import { createTaskCallback } from "./task-callback";
import type {
  AnyHandler,
  QueueLimits,
  RegisteredTask,
  TaskOptions,
  WorkerRunOptions,
} from "./types";
import { createLogger } from "./utils";
import type { WorkflowTracker } from "./workflows";

const log = createLogger("worker");

/** How often the worker heartbeats (with resource health) to storage. */
const HEARTBEAT_INTERVAL_MS = 5000;

/** Outcome kind -> the middleware hook it triggers (events come from
 *  {@link OUTCOME_KIND_EVENTS}). */
const OUTCOME_HOOKS: Record<keyof typeof OUTCOME_KIND_EVENTS, keyof Middleware> = {
  success: "onCompleted",
  retry: "onRetry",
  dead: "onDeadLetter",
  cancelled: "onCancel",
};

/** Inputs assembled by {@link Queue.runWorker}. */
export interface WorkerStartParams {
  tasks: ReadonlyMap<string, RegisteredTask>;
  queueLimits: ReadonlyMap<string, QueueLimits>;
  serializer: Serializer;
  /** Named codec registry for per-task payload decode (see `TaskOptions.codecs`). */
  codecs?: ReadonlyMap<string, PayloadCodec>;
  middleware: readonly Middleware[];
  emitter: Emitter;
  resources: ResourceRuntime;
  /** The queue's shared tracker (undefined on addons without workflows). */
  workflowTracker?: WorkflowTracker;
  /** Flushes the queue's pending topic subscriptions under this worker's id. */
  declareSubscriptions?: (workerId: string) => Promise<void>;
  /** Managed log-topic consumers to drive with a poll loop for the worker's life. */
  logConsumers?: readonly PendingLogConsumer[];
  /** Fired once on stop so the queue can drop this worker from its live set. @internal */
  onStopped?: () => void;
  /** Per-hook budget for the execution middleware, in ms. `0` disables. */
  middlewareTimeoutMs?: number;
  run?: WorkerRunOptions;
}

/** A managed log-topic consumer recorded by `Queue.logConsumer`. */
export interface PendingLogConsumer {
  topic: string;
  name: string;
  handler: AnyHandler;
  pollIntervalMs: number;
  batchSize: number;
  onError: "retry" | "skip";
}

/** A running worker. Hold it for the worker's lifetime; call {@link Worker.stop}. */
export class Worker {
  /** Memoized teardown, set by the first `stop()` — keeps it idempotent. */
  private stopped?: Promise<void>;

  private constructor(
    private readonly native: NativeWorker,
    private readonly queue: NativeQueue,
    private readonly resources: ResourceRuntime,
    private readonly heartbeat: ReturnType<typeof setInterval>,
    private readonly consumerStops: readonly (() => void)[],
    private readonly emitter: Emitter,
    /** Shared with the heartbeat closure so a beat resolving after stop() stays silent. */
    private readonly lifecycle: { stopped: boolean },
    private readonly onStopped?: () => void,
  ) {}

  /**
   * Start a worker from a queue's task registry. Use {@link Queue.runWorker}
   * rather than calling this directly.
   *
   * @internal
   */
  static start(queue: NativeQueue, params: WorkerStartParams): Worker {
    const { tasks, queueLimits, serializer, codecs, middleware, emitter, resources, run } = params;

    // Dashboard-tunable state: per-task middleware disables are re-read on
    // every invocation (live toggles); task/queue overrides apply here, at
    // worker startup.
    const disables = new MiddlewareDisableStore(queue);
    // The job id is unused here: a worker has storage, so it reads the live
    // toggle list by task name rather than taking one off the dispatch.
    const middlewareFor = (taskName: string): readonly Middleware[] => {
      const disabled = disables.getFor(taskName);
      if (disabled.length === 0) {
        return middleware;
      }
      return middleware.filter((mw, index) => !disabled.includes(middlewareKey(mw, index)));
    };

    // Advance workflow runs as node-jobs settle, unless disabled or unsupported.
    const tracker = (run?.advanceWorkflows ?? true) ? (params.workflowTracker ?? null) : null;

    // Durable steps are fenced on `(owner, attempt)`, and the owner is the id
    // *this* worker claims execution under — so sessions are opened through the
    // native worker, not the queue, which two workers would share.
    //
    // The worker does not exist until `runWorker` returns, and its scheduler
    // loop is already dispatching by then, so the callback reaches for it
    // through a holder and waits for the gate rather than reading it empty.
    // Same shape as `Executor.start`, and for the same reason.
    let started: NativeWorker | undefined;
    let markStarted: () => void = () => {};
    const workerReady = new Promise<void>((resolve) => {
      markStarted = resolve;
    });

    const taskCallback = createTaskCallback({
      tasks,
      serializer,
      codecs,
      middlewareFor,
      emitter,
      resources,
      queue,
      middlewareTimeoutMs: params.middlewareTimeoutMs,
      steps: {
        openStepSession: async (jobId, attempt) => {
          await workerReady;
          if (!started) {
            throw new Error("the worker stopped before its step session could open");
          }
          return started.openStepSession(jobId, attempt);
        },
      },
    });

    const outcomeCallback = (outcome: JsOutcome): void => {
      const kind = outcome.kind as keyof typeof OUTCOME_KIND_EVENTS;
      const eventName = OUTCOME_KIND_EVENTS[kind];
      if (!eventName) {
        return;
      }
      const hookName = OUTCOME_HOOKS[kind];
      const event: OutcomeEvent = {
        jobId: outcome.jobId,
        taskName: outcome.taskName,
        queue: outcome.queue ?? undefined,
        error: outcome.error ?? undefined,
        retryCount: outcome.retryCount ?? undefined,
        timedOut: outcome.timedOut ?? undefined,
        durationMs: outcome.durationMs ?? undefined,
      };
      emitter.emit(eventName, event);
      for (const mw of middlewareFor(outcome.taskName)) {
        const hook = mw[hookName] as ((e: OutcomeEvent) => void) | undefined;
        try {
          // Promise.resolve captures async hooks' rejections too.
          void Promise.resolve(hook?.(event)).catch((error) => {
            log.debug(() => `${hookName} middleware hook rejected for ${outcome.jobId}`, error);
          });
        } catch (error) {
          // outcome hook errors must not break the worker
          log.debug(() => `${hookName} middleware hook threw for ${outcome.jobId}`, error);
        }
      }
      tracker?.onOutcome(outcome);
    };

    const nativeOptions: NativeWorkerOptions = {
      queues: run?.queues,
      channelCapacity: run?.channelCapacity,
      concurrency: run?.concurrency,
      batchSize: run?.batchSize,
      // Every registered handler, not just the ones with a policy: the
      // fingerprint on the worker row has to describe what this worker can
      // run, and `taskConfigs` omits every task that took the defaults.
      tasks: [...tasks.keys()],
      taskConfigs: applyTaskOverrides(
        buildTaskConfigs(tasks),
        tasks.keys(),
        new OverridesStore(queue),
      ),
      queueConfigs: applyQueueOverrides(buildQueueConfigs(queueLimits), new OverridesStore(queue)),
      resources: resources.isEmpty ? undefined : resources.names,
      mesh: run?.mesh,
      retention: run?.retention,
      pushDispatch: run?.pushDispatch,
    };
    const native = queue.runWorker(taskCallback, outcomeCallback, nativeOptions);
    started = native;
    markStarted();
    emitter.emit("worker.started", { workerId: native.id, queues: run?.queues });
    // Lease the shared resource runtime only once the native worker actually
    // started, so its worker-scoped values survive until the last worker on this
    // queue stops (see ResourceRuntime). A failed start leaks no lease.
    // The lease also starts the runtime's shared health checker (first lease
    // only) — recreation of failing resources is per runtime, not per worker.
    resources.acquireWorker();

    // Register this worker's topic subscriptions (ephemeral ones under its id)
    // now that the id exists. Registration is idempotent, so a failed flush is
    // retried whole on every heartbeat tick until it succeeds — a silently
    // missing subscription would drop deliveries for the worker's lifetime.
    const flushSubscriptions = params.declareSubscriptions;
    let subscriptionsDeclared = flushSubscriptions === undefined;
    let declarationInFlight = false;
    const declareSubscriptions = (): void => {
      if (subscriptionsDeclared || declarationInFlight || !flushSubscriptions) {
        return;
      }
      declarationInFlight = true;
      void flushSubscriptions(native.id)
        .then(() => {
          subscriptionsDeclared = true;
        })
        .catch((error) => {
          log.error(() => "subscription registration failed; retrying on next heartbeat", error);
        })
        .finally(() => {
          declarationInFlight = false;
        });
    };

    // Heartbeat with current resource health so inspection (and dead-worker
    // reaping) see this worker as alive. Failures are logged, never thrown —
    // the next beat retries. First beat goes out immediately.
    let onlineReported = false;
    const previousUnhealthy = new Set<string>();
    const lifecycle = { stopped: false };
    const sendHeartbeat = (): void => {
      const snapshot = resources.healthSnapshot();
      void queue
        .workerHeartbeat(native.id, snapshot && JSON.stringify(snapshot))
        .then((reapedWorkerIds) => {
          // A beat that resolves after stop() must not emit lifecycle events
          // out of order (clearInterval can't cancel an in-flight promise).
          if (lifecycle.stopped) {
            return;
          }
          // Online = the first heartbeat storage acknowledged, once.
          if (!onlineReported) {
            onlineReported = true;
            emitter.emit("worker.online", { workerId: native.id });
          }
          // The heartbeat doubles as the dead-worker reaper: each reaped peer
          // id is reported as that worker going offline.
          for (const workerId of reapedWorkerIds) {
            emitter.emit("worker.offline", { workerId });
          }
        })
        .catch((error) => {
          log.debug(() => "worker heartbeat failed", error);
        });
      // Report each resource's healthy → unhealthy transition exactly once.
      const unhealthy = new Set(
        Object.entries(snapshot ?? {})
          .filter(([, state]) => state === "unhealthy")
          .map(([name]) => name),
      );
      for (const resource of unhealthy) {
        if (!previousUnhealthy.has(resource)) {
          emitter.emit("worker.unhealthy", { workerId: native.id, resource });
        }
      }
      previousUnhealthy.clear();
      for (const resource of unhealthy) {
        previousUnhealthy.add(resource);
      }
      // Same cadence, same reaper election: passing this worker's id gates the
      // sweep so only the leader runs it. Per-tick failures are swallowed like
      // the heartbeat's — the next beat retries.
      void queue.reapEphemeralSubscriptions(native.id).catch((error) => {
        log.debug(() => "ephemeral subscription reap failed", error);
      });
      declareSubscriptions();
    };
    sendHeartbeat();
    const heartbeat = setInterval(sendHeartbeat, run?.heartbeatIntervalMs ?? HEARTBEAT_INTERVAL_MS);
    heartbeat.unref();

    // Managed log-topic consumers: one poll loop each, beside the heartbeat.
    const consumerStops = startLogConsumers(queue, serializer, params.logConsumers ?? []);

    return new Worker(
      native,
      queue,
      resources,
      heartbeat,
      consumerStops,
      emitter,
      lifecycle,
      params.onStopped,
    );
  }

  /**
   * Stop the worker; in-flight results drain before background tasks exit.
   *
   * Dispatch, the heartbeat and the log consumers halt synchronously, so
   * ignoring the return value behaves exactly as a void `stop()` would. The
   * returned promise resolves once worker-scoped resources have been disposed
   * — await it when that matters (test teardown, graceful shutdown). It never
   * rejects: teardown failures are logged, not thrown.
   *
   * Idempotent: later calls return the first teardown. Re-running it would
   * release a second resource lease and tear down another worker's resources.
   */
  stop(): Promise<void> {
    if (!this.stopped) {
      let settle!: () => void;
      // Install the shared promise BEFORE teardown runs: `onStopped` and the
      // `worker.stopped` listeners fire synchronously inside runStop(), and
      // either may call stop() again — a reentrant call has to see this
      // promise rather than start a second teardown.
      this.stopped = new Promise<void>((resolve) => {
        settle = resolve;
      });
      try {
        void this.runStop().then(settle, settle);
      } catch (error) {
        log.debug(() => "worker stop failed", error);
        settle();
      }
    }
    return this.stopped;
  }

  private runStop(): Promise<void> {
    this.lifecycle.stopped = true;
    this.onStopped?.();
    // One last sweep for orphaned ephemeral subscriptions before this worker's
    // reap cadence goes away. Best effort — stopping must never throw.
    void this.queue.reapEphemeralSubscriptions().catch((error) => {
      log.debug(() => "final ephemeral subscription reap failed", error);
    });
    clearInterval(this.heartbeat);
    for (const stop of this.consumerStops) {
      stop();
    }
    this.native.stop();
    this.emitter.emit("worker.stopped", { workerId: this.native.id });
    // Dispose worker-scoped resources after the native worker quiesces (the
    // teardown drains the runtime's health checker before touching caches).
    // Best effort: lazy resources mean this is a no-op when none were built.
    return this.resources.teardownWorker().catch((error) => {
      log.debug(() => "worker-scope resource teardown failed", error);
    });
  }
}

/**
 * Ask a task's `retryOn` predicate whether `error` is worth retrying. No
 * predicate means retry, and so does one that throws — a broken classifier must
 * not silently turn transient failures into dead letters.
 */
/** Start one poll loop per managed consumer; return their timers to clear on stop. */
function startLogConsumers(
  queue: NativeQueue,
  serializer: Serializer,
  consumers: readonly PendingLogConsumer[],
): (() => void)[] {
  const stops: (() => void)[] = [];
  for (const consumer of consumers) {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const schedule = (delayMs: number): void => {
      if (stopped) {
        return;
      }
      timer = setTimeout(runOnce, delayMs);
      timer.unref();
    };
    // Self-scheduling loop (not a fixed-cadence setInterval): after a batch that
    // made progress, re-read immediately to drain a backlog; only wait the poll
    // interval when caught up (empty) or backing off a retry poison.
    const runOnce = (): void => {
      void drainLogConsumerBatch(queue, serializer, consumer)
        .then((outcome) => {
          schedule(outcome === "drained" ? 0 : consumer.pollIntervalMs);
        })
        .catch((error) => {
          log.error(() => `log consumer ${consumer.topic}/${consumer.name} poll failed`, error);
          schedule(consumer.pollIntervalMs);
        });
    };
    stops.push(() => {
      stopped = true;
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    });
    runOnce(); // read immediately rather than waiting a full interval
  }
  return stops;
}

/** `empty` = nothing to read (wait the poll interval); `drained` = made progress,
 *  re-read immediately; `retry-backoff` = a retry-mode handler failure blocked the
 *  cursor, so wait one interval before re-reading rather than hot-looping. */
type DrainOutcome = "empty" | "drained" | "retry-backoff";

/** One poll: read a batch, invoke the handler per message, then advance the cursor.
 *  `retry` stops at the first failure and acks only the successes before it (and
 *  backs off); `skip` acks past a failure and keeps going. Payload decode runs
 *  inside the per-message guard so a bad payload obeys the same error policy. */
async function drainLogConsumerBatch(
  queue: NativeQueue,
  serializer: Serializer,
  consumer: PendingLogConsumer,
): Promise<DrainOutcome> {
  const messages = await queue.readTopicMessages(consumer.topic, consumer.name, consumer.batchSize);
  if (messages.length === 0) {
    return "empty";
  }
  let lastAcked: string | undefined;
  let retryFailure = false;
  for (const message of messages) {
    try {
      const args = deserializeCall(serializer, message.payload);
      await consumer.handler(...args);
    } catch (error) {
      log.error(
        () => `log consumer ${consumer.topic}/${consumer.name} handler failed on ${message.id}`,
        error,
      );
      if (consumer.onError === "retry") {
        retryFailure = true;
        break;
      }
    }
    lastAcked = message.id;
  }
  if (lastAcked !== undefined) {
    await queue.ackTopicCursor(consumer.topic, consumer.name, lastAcked);
  }
  return retryFailure ? "retry-backoff" : "drained";
}

/** Collect per-task configs that actually set something. */
function buildTaskConfigs(tasks: ReadonlyMap<string, RegisteredTask>): TaskConfigInput[] {
  const configs: TaskConfigInput[] = [];
  for (const [name, task] of tasks) {
    if (!task.options) {
      continue;
    }
    const config = toTaskConfig(name, task.options);
    if (setsSomething(config)) {
      configs.push(config);
    }
  }
  return configs;
}

function toTaskConfig(name: string, options: TaskOptions): TaskConfigInput {
  return {
    name,
    maxRetries: options.maxRetries,
    retryBaseDelayMs: options.retryBackoff?.baseMs,
    retryMaxDelayMs: options.retryBackoff?.maxMs,
    maxConcurrent: options.maxConcurrent,
    maxInFlightPerTask: options.maxInFlightPerTask,
    rateLimit: options.rateLimit,
    onExcess: options.onExcess,
    retryBudget: options.retryBudget,
    circuitBreaker: options.circuitBreaker,
  };
}

/**
 * Whether a task set any policy worth registering.
 *
 * Derived from the built config rather than a hand-listed set of option names:
 * a list silently drops any option missing from it, so a task setting only the
 * new option would never reach the scheduler — with no error, and invisible to
 * type-checking. `name` is always present, so it can't stand in for a setting.
 */
function setsSomething({ name: _name, ...policy }: TaskConfigInput): boolean {
  return Object.values(policy).some((value) => value !== undefined);
}

function buildQueueConfigs(limits: ReadonlyMap<string, QueueLimits>): QueueConfigInput[] {
  return [...limits].map(([name, limit]) => ({
    name,
    maxConcurrent: limit.maxConcurrent,
    rateLimit: limit.rateLimit,
    codelTargetMs: limit.codel?.targetMs,
    codelIntervalMs: limit.codel?.intervalMs,
    dispatchOrder: limit.dispatchOrder,
  }));
}
