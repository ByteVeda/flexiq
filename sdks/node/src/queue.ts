import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import {
  MiddlewareDisableStore,
  middlewareKey,
  OverridesStore,
  type QueueOverride,
  type TaskOverride,
} from "./dashboard/stores";
import {
  InterceptionError,
  JobCancelledError,
  JobFailedError,
  LockLostError,
  LockNotAcquiredError,
  PredicateRejectedError,
  QueueError,
  QueueFullError,
  ResourceError,
  ResultTimeoutError,
  SerializationError,
  TaskitoError,
} from "./errors";
import { Emitter, type EventMap, type EventName } from "./events";
import { type Interception, InterceptionMetrics, type Interceptor } from "./interception";
import { Lock, type LockOptions } from "./locks";
import type { EnqueueContext, Middleware } from "./middleware";
import {
  JsQueue,
  type JsTopicMessage,
  type EnqueueOptions as NativeEnqueueOptions,
  type NativeQueue,
  type OpenOptions,
} from "./native";
import { encodeNotes } from "./notes";
import type { Predicate } from "./predicates";
import { type ProxyHandlerStats, proxyMetrics } from "./proxies";
import {
  type PoolOptions,
  type ResourceContext,
  type ResourceMetrics,
  ResourceRuntime,
  type ResourceScope,
} from "./resources";
import { parseEffectiveRetention, parseRetentionPreview } from "./retention";
import {
  CodecSerializer,
  deserializeCall,
  JsonSerializer,
  type PayloadCodec,
  type Serializer,
  serializeCall,
} from "./serializers";
import type {
  AnyHandler,
  CircuitBreaker,
  CursorDetailedJobFilter,
  CursorJobFilter,
  DeadJob,
  DeclaredTopic,
  DetailedJobFilter,
  EffectiveRetention,
  EnqueueOptions,
  Job,
  JobDag,
  JobError,
  JobFilter,
  LogConsumerOptions,
  Metric,
  Page,
  PeriodicOptions,
  PeriodicTask,
  PublishOptions,
  QueueLimits,
  RegisteredTask,
  ReplayEntry,
  ResultOptions,
  RetentionOptions,
  RetentionPreview,
  Stats,
  StreamOptions,
  SubscriberOptions,
  Subscription,
  TaskLog,
  TaskLogLevel,
  TaskLogLevelFilter,
  TaskMap,
  TaskOptions,
  TopicLogStat,
  TopicMessage,
  TopicStat,
  WorkerInfo,
  WorkerRunOptions,
} from "./types";
import { createLogger } from "./utils";
import { WebhookManager } from "./webhooks";
import { type PendingLogConsumer, Worker } from "./worker";
import { WorkflowManager } from "./workflows";
import { WorkflowTracker } from "./workflows/tracker";

const log = createLogger("queue");

/** Construction options for a {@link Queue}. */
export interface QueueOptions {
  /** SQLite file path — shorthand for `{ backend: "sqlite", dsn: path }`.
   *  Defaults to `.taskito/taskito.db`; missing parent directories are created. */
  dbPath?: string;
  /** `"sqlite"` (default), `"postgres"`, or `"redis"`. */
  backend?: "sqlite" | "postgres" | "redis";
  /** Backend connection string (SQLite path, Postgres URL, Redis URL). */
  dsn?: string;
  /** Connection pool size (SQLite/Postgres). */
  poolSize?: number;
  /** Postgres schema (default `"taskito"`). */
  schema?: string;
  /** Redis key prefix. */
  prefix?: string;
  /** Namespace applied to enqueued jobs and the worker scheduler. */
  namespace?: string;
  /** Codec for task args/results. Defaults to {@link JsonSerializer}. */
  serializer?: Serializer;
  /**
   * Global payload codec chain, applied in order after serialization and in
   * reverse before deserialization. Wraps the queue serializer, so it covers
   * every payload and result. Jobs persisted before the chain was enabled
   * cannot be decoded through it.
   */
  codec?: PayloadCodec | PayloadCodec[];
  /**
   * Named codec registry for per-task codecs. Tasks opt in via
   * {@link TaskOptions.codecs}; applies to task payloads only (results stay
   * on the queue serializer).
   */
  codecs?: Record<string, PayloadCodec>;
}

/**
 * A Taskito queue: register tasks, enqueue work, read results, and run workers.
 * Backed by the Rust core over SQLite, Postgres, or Redis.
 */
/**
 * Normalize a native page to the declared {@link Page} shape.
 *
 * napi types an `Option<String>` as `nextCursor?: string` but hands back `null`
 * at runtime, so the two disagree: a `=== undefined` check against the raw value
 * would silently never fire. Settle on `null`, matching the other SDKs.
 */
function toPage<T>(page: { items: T[]; nextCursor?: string | null }): Page<T> {
  return { items: page.items, nextCursor: page.nextCursor ?? null };
}

export class Queue<TTasks extends TaskMap = TaskMap> {
  private readonly native: NativeQueue;
  private readonly serializer: Serializer;
  private readonly codecs: ReadonlyMap<string, PayloadCodec>;
  private readonly tasks = new Map<string, RegisteredTask>();
  private readonly pendingSubscriptions: PendingSubscription[] = [];
  private readonly pendingLogConsumers: PendingLogConsumer[] = [];
  private readonly queueLimits = new Map<string, QueueLimits>();
  private readonly middleware: Middleware[] = [];
  private readonly interceptors: Interceptor[] = [];
  private readonly interceptionMetrics = new InterceptionMetrics();
  private readonly gates = new Map<string, Predicate[]>();
  private readonly emitter = new Emitter();
  private readonly resources = new ResourceRuntime();
  private readonly webhookManager: WebhookManager;
  /** Built lazily — its constructor throws on addons lacking the `workflows` feature. */
  private workflowManager?: WorkflowManager;
  /** Shared by workers and `workflows.resolveGate()` so gate timers clear. */
  private workflowTracker?: WorkflowTracker;

  constructor(options: QueueOptions = {}) {
    this.native = JsQueue.open(toOpenOptions(options));
    const chain = options.codec === undefined ? [] : [options.codec].flat();
    const baseSerializer = options.serializer ?? new JsonSerializer();
    this.serializer =
      chain.length > 0 ? new CodecSerializer(baseSerializer, chain) : baseSerializer;
    this.codecs = new Map(Object.entries(options.codecs ?? {}));
    this.webhookManager = new WebhookManager(this.native, this.emitter);
  }

  /** Webhook subscriptions — create/list/delete and deliver job events to URLs. */
  get webhooks(): WebhookManager {
    return this.webhookManager;
  }

  /** Workflow definitions and runs — DAG/linear orchestration over the queue. */
  get workflows(): WorkflowManager {
    if (!this.workflowManager) {
      this.workflowManager = new WorkflowManager(
        this.native,
        this.serializer,
        this.trackerIfSupported(),
        (taskName, value) => this.encodeTaskPayload(taskName, value),
        this.emitter,
      );
    }
    return this.workflowManager;
  }

  /** The shared workflow tracker, or `undefined` on addons without workflows. */
  private trackerIfSupported(): WorkflowTracker | undefined {
    if (typeof this.native.markWorkflowNodeResult !== "function") {
      return undefined;
    }
    this.workflowTracker ??= new WorkflowTracker(
      this.native,
      this.serializer,
      (taskName, args) => this.encodeTaskPayload(taskName, args),
      this.emitter,
    );
    return this.workflowTracker;
  }

  /** Create a distributed lock handle (not yet acquired). */
  lock(name: string, options?: LockOptions): Lock {
    return new Lock(this.native, name, options);
  }

  /**
   * Run `fn` while holding the named lock, releasing it afterwards. Rejects with
   * {@link LockNotAcquiredError} if another owner holds the lock.
   */
  async withLock<T>(name: string, fn: () => T | Promise<T>, options?: LockOptions): Promise<T> {
    const lock = this.lock(name, options);
    if (!lock.acquire()) {
      throw new LockNotAcquiredError(name);
    }
    let result: T;
    try {
      result = await fn();
    } catch (error) {
      lock.release();
      throw error;
    }
    // `release()` returns false when the lease was lost mid-run — surface that
    // rather than pretending the critical section ran under a held lock.
    if (!lock.release()) {
      throw new LockLostError(name);
    }
    return result;
  }

  /**
   * Register (or replace) a cron-scheduled task. A running worker enqueues
   * `taskName` with the serialized `args` each time the schedule fires. Returns
   * the next fire time (Unix ms); throws on an invalid cron expression.
   */
  registerPeriodic(
    name: string,
    taskName: string,
    cronExpr: string,
    options?: PeriodicOptions,
  ): number {
    const args = this.encodeTaskPayload(taskName, options?.args ?? []);
    return this.native.registerPeriodic(
      name,
      taskName,
      cronExpr,
      args,
      options?.queue,
      options?.timezone,
      options?.enabled,
    );
  }

  /** Every registered periodic task, enabled or paused. */
  listPeriodic(): PeriodicTask[] {
    return this.native.listPeriodic();
  }

  /** Unschedule a periodic task. Returns false if none had that name. */
  deletePeriodic(name: string): boolean {
    return this.native.deletePeriodic(name);
  }

  /** Stop a periodic task from firing without removing it; false if none had that name. */
  pausePeriodic(name: string): boolean {
    return this.native.setPeriodicEnabled(name, false);
  }

  /** Resume a paused periodic task; false if none had that name. */
  resumePeriodic(name: string): boolean {
    return this.native.setPeriodicEnabled(name, true);
  }

  /**
   * Register a task that receives injected resources as a trailing `deps` object
   * (`handler(...args, deps)`). The `deps` param is stripped from the typed
   * {@link Queue.enqueue} args. Annotate `deps` to type the injected resources.
   */
  task<Name extends string, A extends unknown[], D, R>(
    name: Name,
    handler: (...args: [...A, deps: D]) => R | Promise<R>,
    options: TaskOptions & { inject: readonly string[] },
  ): Queue<TTasks & Record<Name, (...args: A) => R>>;
  /**
   * Register a task handler under `name`. Chain calls to build a typed registry —
   * {@link Queue.enqueue} then infers each task's argument types.
   */
  task<Name extends string, Handler extends AnyHandler>(
    name: Name,
    handler: Handler,
    options?: TaskOptions,
  ): Queue<TTasks & Record<Name, Handler>>;
  task(name: string, handler: AnyHandler, options?: TaskOptions): Queue<TaskMap> {
    this.tasks.set(name, { handler, options });
    return this as unknown as Queue<TaskMap>;
  }

  /**
   * Register `handler` as an independent subscriber of `topic`. It becomes a
   * normal task named `name` (so retries, DLQ, middleware, and rate limits all
   * apply per subscriber), and the subscription is written to storage when a
   * worker starts — or via {@link Queue.declareSubscriptions} in a
   * producer-only process. `durable: false` ties the subscription to one
   * worker: it only registers inside a running worker and is reaped once that
   * worker stops heartbeating.
   */
  subscriber<Name extends string, Handler extends AnyHandler>(
    topic: string,
    name: Name,
    handler: Handler,
    options?: SubscriberOptions,
  ): Queue<TTasks & Record<Name, Handler>> {
    const { subscriptionName, queue, durable, ...taskOptions } = options ?? {};
    // publish() encodes one shared payload with the queue serializer only, but
    // the worker would reverse a per-task codec chain — a guaranteed decode
    // failure, so reject it up front.
    if (taskOptions.codecs && taskOptions.codecs.length > 0) {
      throw new QueueError(
        `subscriber "${name}": per-task codecs do not apply to topic deliveries — ` +
          "published payloads use the queue-level serializer only",
      );
    }
    const pending: PendingSubscription = {
      topic,
      subscriptionName: subscriptionName ?? name,
      taskName: name,
      queue: queue ?? "default",
      durable: durable ?? true,
    };
    // Redeclaring the same (topic, subscriptionName) replaces the pending
    // entry — declareSubscriptions must stay idempotent.
    const existing = this.pendingSubscriptions.findIndex(
      (sub) => sub.topic === topic && sub.subscriptionName === pending.subscriptionName,
    );
    if (existing >= 0) {
      this.pendingSubscriptions[existing] = pending;
    } else {
      this.pendingSubscriptions.push(pending);
    }
    return this.task(name, handler, taskOptions);
  }

  /**
   * Register a **managed consumer** of log `topic`: a durable log subscription
   * plus, once a worker runs, a poll loop that pulls messages, invokes
   * `handler(...args)` per message, and advances the cursor — the
   * `readTopic`/`ackTopic` loop callers otherwise write by hand. The handler may
   * be sync or async. Registration is eager (like {@link Queue.subscribeLog}) so
   * a producer-only process still retains the topic's publishes.
   */
  logConsumer(
    topic: string,
    name: string,
    handler: AnyHandler,
    options?: LogConsumerOptions,
  ): this {
    const consumer: PendingLogConsumer = {
      topic,
      name,
      handler,
      pollIntervalMs: options?.pollIntervalMs ?? 1000,
      batchSize: options?.batchSize ?? 100,
      onError: options?.onError ?? "retry",
    };
    // Replace, don't append: re-registering the same (topic, name) must not
    // spawn a duplicate poll loop.
    const existing = this.pendingLogConsumers.findIndex(
      (c) => c.topic === topic && c.name === name,
    );
    if (existing >= 0) {
      this.pendingLogConsumers[existing] = consumer;
    } else {
      this.pendingLogConsumers.push(consumer);
    }
    // Eagerly register the durable cursor; the worker also re-registers it
    // (idempotent) so a late/failed flush can't silently drop deliveries.
    void this.subscribeLog(topic, name).catch((error) => {
      // Best effort — the worker re-asserts this on start. Log so a persistent
      // failure (bad topic, storage error) isn't invisible until then.
      log.error(() => `log consumer ${topic}/${name}: eager subscribe failed`, error);
    });
    return this;
  }

  /**
   * Register an injectable resource. Worker-scoped (default) values are built
   * once and shared across the worker's lifetime; task-scoped values are built
   * per job invocation; pooled values are checked out of a bounded pool per job
   * and returned when it finishes (tune via `pool`). Reach them from a handler
   * via `useResource(name)` or the declarative `inject` option on
   * {@link Queue.task}.
   */
  resource<T>(
    name: string,
    factory: (ctx: ResourceContext) => T | Promise<T>,
    options?: {
      scope?: ResourceScope;
      dispose?: (value: T) => void | Promise<void>;
      pool?: PoolOptions;
      /** Returns truthy while healthy; failures trigger recreation. Worker scope only. */
      healthCheck?: (value: T) => boolean | Promise<boolean>;
      /** Milliseconds between health checks. 0 or absent disables checking. */
      healthCheckIntervalMs?: number;
      /** Failed checks tolerated (while recreation also fails) before the
       * resource is marked permanently unhealthy. Default 3. */
      maxRecreationAttempts?: number;
    },
  ): this {
    const scope = options?.scope ?? "worker";
    if (options?.pool && scope !== "pooled") {
      throw new ResourceError(
        `Resource "${name}": pool options require scope "pooled" (got "${scope}")`,
      );
    }
    if (options?.healthCheck && scope !== "worker") {
      throw new ResourceError(
        `Resource "${name}": health checks require scope "worker" (got "${scope}") — ` +
          "task and pooled instances are already rebuilt or recycled per job",
      );
    }
    this.resources.register<T>(name, {
      factory,
      scope,
      dispose: options?.dispose,
      pool: options?.pool,
      healthCheck: options?.healthCheck,
      healthCheckIntervalMs: options?.healthCheckIntervalMs,
      maxRecreationAttempts: options?.maxRecreationAttempts,
    });
    return this;
  }

  /** Per-resource lifecycle metrics (created / disposed / active), keyed by name. */
  resourceMetrics(): ResourceMetrics {
    return this.resources.metrics();
  }

  /** Set per-queue concurrency / rate-limit applied when a worker runs. */
  configureQueue(name: string, limits: QueueLimits): void {
    // A negative cap would make `pending + incoming > cap` always true and
    // permanently reject every enqueue — reject it at configuration time.
    if (limits.maxPending !== undefined && limits.maxPending < 0) {
      throw new RangeError("maxPending must be non-negative");
    }
    // CoDel bounds cross to native as i64 — reject non-positive/non-integer
    // (0, negatives, fractions, NaN, Infinity) here rather than silently coercing.
    if (limits.codel !== undefined) {
      for (const key of ["targetMs", "intervalMs"] as const) {
        const value = limits.codel[key];
        if (!Number.isInteger(value) || value <= 0) {
          throw new RangeError(`codel.${key} must be a positive integer`);
        }
      }
    }
    // Snapshot after validating: `limits` is caller-owned, so store a copy (with
    // a copied `codel`) so a later mutation can't slip past the checks above.
    this.queueLimits.set(name, {
      ...limits,
      codel: limits.codel === undefined ? undefined : { ...limits.codel },
    });
  }

  /** Register middleware (execution + outcome hooks). Runs in registration order. */
  use(middleware: Middleware): void {
    this.middleware.push(middleware);
  }

  /**
   * Gate enqueues of `name` with a predicate evaluated at enqueue time (after
   * `onEnqueue`). If it returns `false`, the enqueue throws
   * {@link PredicateRejectedError}. Multiple gates on a task all must pass.
   */
  /**
   * Register an enqueue interceptor. Interceptors run at the start of every
   * enqueue — before defaults, middleware, and gates — chained in
   * registration order, each seeing the previous one's task name and args.
   * Returning `Interception.reject(...)` (or a null-ish value) makes the
   * enqueue throw {@link InterceptionError}; `redirect` is not supported for
   * batch enqueue or for tasks with per-task codecs.
   */
  intercept(interceptor: Interceptor): this {
    this.interceptors.push(interceptor);
    return this;
  }

  gate<Name extends keyof TTasks & string>(
    name: Name,
    predicate: (ctx: { taskName: Name; args: Parameters<TTasks[Name]> }) => boolean,
  ): this {
    const list = this.gates.get(name) ?? [];
    list.push(predicate as Predicate);
    this.gates.set(name, list);
    return this;
  }

  /** Subscribe to a queue event (job, worker, queue, workflow, predicate). */
  on<E extends EventName>(event: E, handler: (event: EventMap[E]) => void): void {
    this.emitter.on(event, handler);
  }

  /** Unsubscribe from a queue event. */
  off<E extends EventName>(event: E, handler: (event: EventMap[E]) => void): void {
    this.emitter.off(event, handler);
  }

  /** Enqueue `name` with positional `args` (typed per the registered task). Returns the job id.
   * `args` stays optional so the in-place `queue.task(...)` registration pattern (where the
   * variable's type isn't refined) keeps working for zero-arg tasks. */
  enqueue<Name extends keyof TTasks & string>(
    name: Name,
    args?: Parameters<TTasks[Name]>,
    options?: EnqueueOptions,
  ): string {
    this.rejectIfQueueFull(options?.queue ?? "default");
    const { taskName, payload, options: nativeOpts } = this.prepareEnqueue(name, args, options);
    const jobId = this.native.enqueue(taskName, payload, nativeOpts);
    this.emitter.emit("job.enqueued", { jobId, taskName, queue: nativeOpts.queue ?? "default" });
    return jobId;
  }

  /**
   * Enforce the opt-in `maxPending` admission cap for a queue (set via
   * {@link Queue.configureQueue}). Throws {@link QueueFullError} when admitting
   * `incoming` jobs would push the queue's pending backlog past its cap;
   * `incoming` is the batch size, so a batch is rejected as a whole rather than
   * overshooting the cap by its full size. A no-op (and no query) for uncapped
   * queues. Non-atomic count-then-insert, like the rate limiter.
   */
  private rejectIfQueueFull(queue: string, incoming = 1): void {
    const cap = this.queueLimits.get(queue)?.maxPending;
    if (cap === undefined) return;
    const pending = this.native.countPendingByQueue(queue);
    if (pending + incoming > cap) {
      throw new QueueFullError(queue, pending, cap);
    }
  }

  /**
   * Enqueue many jobs of `name` in one storage round-trip. Each entry is its own
   * typed `args` + `options`. Returns the new job ids in input order. Entries
   * carrying a `uniqueKey` dedup exactly like {@link Queue.enqueue}: a key that
   * already has a pending/running job yields that job's id instead of a new row.
   */
  enqueueMany<Name extends keyof TTasks & string>(
    name: Name,
    jobs: ReadonlyArray<{ args?: Parameters<TTasks[Name]>; options?: EnqueueOptions }>,
  ): string[] {
    // All-or-nothing: reject the batch if admitting it would push a target queue
    // past its cap. Count rows per queue so the check accounts for the whole
    // batch rather than overshooting the cap by its size.
    const perQueue = new Map<string, number>();
    for (const job of jobs) {
      const queue = job.options?.queue ?? "default";
      perQueue.set(queue, (perQueue.get(queue) ?? 0) + 1);
    }
    for (const [queue, incoming] of perQueue) {
      this.rejectIfQueueFull(queue, incoming);
    }
    const prepared = jobs.map((job) => {
      const { payload, options } = this.prepareEnqueue(name, job.args, job.options, {
        batch: true,
      });
      return { payload, options };
    });
    const jobIds = this.native.enqueueMany(name, prepared);
    // One event per created job, in input order. The batch path rejects
    // redirects, so every job carries the caller's task name.
    jobIds.forEach((jobId, index) => {
      this.emitter.emit("job.enqueued", {
        jobId,
        taskName: name,
        queue: prepared[index]?.options.queue ?? "default",
      });
    });
    return jobIds;
  }

  // ── Topic pub/sub ─────────────────────────────────────────────────

  /**
   * Publish a message to `topic`: every active subscription receives an
   * independent job carrying the same serialized `args` (at-least-once per
   * subscriber). Returns the created jobs — empty when the topic has no
   * active subscribers, a valid pub/sub no-op. `idempotencyKey` dedupes per
   * subscriber. Deliveries use the queue-level serializer; per-task codecs
   * do not apply.
   */
  publish(topic: string, args: unknown[] = [], options?: PublishOptions): Promise<Job[]> {
    const { notes, ...rest } = options ?? {};
    const payload = Buffer.from(serializeCall(this.serializer, args));
    return this.native.publish(topic, payload, {
      ...rest,
      notes: notes === undefined ? undefined : encodeNotes(notes),
    });
  }

  /**
   * Write pending durable subscriptions to storage. Runs automatically at
   * worker startup; call it explicitly in a producer-only process (one that
   * registers subscribers but never runs a worker) so `publish()` sees them.
   * Ephemeral subscriptions are skipped — they need an owning worker.
   *
   * Managed log consumers are flushed too: `logConsumer()` registers their
   * cursor eagerly but does not await it, and `publish()` only retains a log
   * message once the log subscription exists. Awaiting this before the first
   * publish is what makes that retention deterministic.
   */
  async declareSubscriptions(): Promise<void> {
    for (const subscription of this.pendingSubscriptions) {
      if (subscription.durable) {
        await this.registerSubscription(subscription, undefined);
      }
    }
    await this.declareLogConsumers();
  }

  /** Remove a subscription. Resolves false if none matched. */
  unsubscribe(topic: string, name: string): Promise<boolean> {
    // Drop any matching pending entry too, so a later declareSubscriptions()
    // or worker start doesn't resurrect the removed subscription.
    const pending = this.pendingSubscriptions.findIndex(
      (sub) => sub.topic === topic && sub.subscriptionName === name,
    );
    if (pending >= 0) {
      this.pendingSubscriptions.splice(pending, 1);
    }
    return this.native.unsubscribe(topic, name);
  }

  /** Stop deliveries without unregistering. Resolves false if unknown. */
  pauseSubscription(topic: string, name: string): Promise<boolean> {
    return this.native.setSubscriptionActive(topic, name, false);
  }

  /** Resume a paused subscription. Resolves false if unknown. */
  resumeSubscription(topic: string, name: string): Promise<boolean> {
    return this.native.setSubscriptionActive(topic, name, true);
  }

  /** List subscriptions — all of them, or one topic's active ones. */
  listSubscriptions(topic?: string): Promise<Subscription[]> {
    return this.native.listSubscriptions(topic);
  }

  /**
   * Backlog snapshot per subscription, optionally filtered to one `topic`. Every
   * registered subscription appears — paused or ephemeral ones included — even
   * with nothing queued, so the full subscriber list comes from one call.
   * Counts are computed live off indexed columns, so this is safe to poll.
   */
  async topicStats(topic?: string): Promise<TopicStat[]> {
    const stats = await this.native.topicBacklogStats();
    return topic === undefined ? stats : stats.filter((stat) => stat.topic === topic);
  }

  /**
   * Register a durable **log** subscription: a named cursor over `topic`. Unlike
   * `subscriber`, it has no handler — the topic's publishes are stored once each
   * and this consumer pulls them with `readTopic`, advancing with `ackTopic`.
   * Writes immediately, so register it before the publishes it should see.
   */
  subscribeLog(topic: string, name: string): Promise<void> {
    return this.native.registerSubscription(
      topic,
      name,
      "",
      "default",
      true,
      undefined,
      undefined,
      undefined,
      undefined,
      "log",
    );
  }

  /**
   * Pull up to `limit` messages after a log subscription's cursor, oldest first
   * and exclusive of it. Each `args` is the deserialized publish payload. Empty
   * when caught up. At-least-once: process, then `ackTopic` the last `id`.
   */
  async readTopic(topic: string, name: string, limit = 100): Promise<TopicMessage[]> {
    const messages = await this.native.readTopicMessages(topic, name, limit);
    return messages.map((msg) => this.decodeTopicMessage(msg));
  }

  /**
   * Advance a log subscription's cursor to `cursor` (a message id). A high-water
   * mark — acking an id acks everything up to it. Monotonic; resolves false when
   * nothing moved.
   */
  ackTopic(topic: string, name: string, cursor: string): Promise<boolean> {
    return this.native.ackTopicCursor(topic, name, cursor);
  }

  /**
   * Lease up to `opts.limit` (default 100) messages for **per-message**
   * consumption. Unlike `readTopic`'s cursor, each message is leased for
   * `opts.visibility` seconds (default 30) and tracked individually: `ackMessage`
   * it when done, or `nackMessage` to redeliver it now. A lease that expires
   * un-acked is redelivered, so one poison message no longer blocks its siblings.
   * In-flight (leased, un-expired) messages are skipped; oldest first.
   */
  async leaseTopic(
    topic: string,
    name: string,
    opts?: { limit?: number; visibility?: number },
  ): Promise<TopicMessage[]> {
    const visibilityMs = Math.round((opts?.visibility ?? 30) * 1000);
    const messages = await this.native.leaseTopicMessages(
      topic,
      name,
      opts?.limit ?? 100,
      visibilityMs,
    );
    return messages.map((msg) => this.decodeTopicMessage(msg));
  }

  /**
   * Ack one leased message — the delivery is done and never redelivered. Resolves
   * false when there was no un-acked delivery to ack.
   */
  ackMessage(topic: string, name: string, messageId: string): Promise<boolean> {
    return this.native.ackMessage(topic, name, messageId);
  }

  /**
   * Nack one leased message — make it available for redelivery now (vs waiting out
   * the visibility timeout). Resolves false when there was no un-acked delivery.
   */
  nackMessage(topic: string, name: string, messageId: string): Promise<boolean> {
    return this.native.nackMessage(topic, name, messageId);
  }

  /** Decode a native log message into a {@link TopicMessage} (shared by read/lease). */
  private decodeTopicMessage(msg: JsTopicMessage): TopicMessage {
    return {
      id: msg.id,
      args: deserializeCall(this.serializer, msg.payload),
      metadata: msg.metadata === undefined ? undefined : JSON.parse(msg.metadata),
      notes: msg.notes === undefined ? undefined : JSON.parse(msg.notes),
      createdAt: msg.createdAt,
    };
  }

  /** Lag snapshot per log subscription. */
  topicLogStats(): Promise<TopicLogStat[]> {
    return this.native.topicLogStats();
  }

  /**
   * Drop ephemeral subscriptions whose owning worker is gone. Workers run
   * this on their heartbeat cadence; exposed for operational tooling.
   * Resolves to the number of subscriptions removed.
   */
  reapEphemeralSubscriptions(): Promise<number> {
    return this.native.reapEphemeralSubscriptions();
  }

  /** Distinct topics that currently have at least one subscription. */
  async listTopics(): Promise<string[]> {
    const topics = new Set<string>();
    for (const subscription of await this.native.listSubscriptions(undefined)) {
      topics.add(subscription.topic);
    }
    return [...topics];
  }

  /**
   * Declare a **log** topic so its publishes are retained even with no
   * subscriber (removing the late-join boundary). Without a declaration, a log
   * message is stored only when a log subscription already exists at publish
   * time; declaring the topic makes every publish durable, so a consumer that
   * subscribes later still sees them.
   *
   * `retention` (seconds) bounds a sub-less backlog: each stored message expires
   * that long after it was published, so the retention sweep can reclaim it.
   * Omit it to keep messages until a subscriber consumes them. Idempotent —
   * re-declaring updates the retention window.
   */
  declareTopic(name: string, opts?: { retention?: number }): Promise<void> {
    return this.native.declareTopic(
      name,
      "log",
      opts?.retention === undefined ? undefined : Math.round(opts.retention * 1000),
    );
  }

  /** List declared topics: `name`, `mode`, `retentionMs` (absent = unbounded), `createdAt` (Unix ms). */
  listDeclaredTopics(): Promise<DeclaredTopic[]> {
    return this.native.listDeclaredTopics();
  }

  /** Flush every pending subscription at worker startup, owning the ephemeral ones. */
  private async declareWorkerSubscriptions(workerId: string): Promise<void> {
    for (const subscription of this.pendingSubscriptions) {
      await this.registerSubscription(subscription, subscription.durable ? undefined : workerId);
    }
    await this.declareLogConsumers();
  }

  /** Re-assert managed consumers' durable log subscriptions (idempotent) in case
   *  the eager registration in logConsumer() lost a race or failed. */
  private async declareLogConsumers(): Promise<void> {
    for (const consumer of this.pendingLogConsumers) {
      await this.subscribeLog(consumer.topic, consumer.name);
    }
  }

  private registerSubscription(
    subscription: PendingSubscription,
    ownerWorkerId: string | undefined,
  ): Promise<void> {
    // Persist the subscriber's own delivery settings on the subscription row so
    // a producer-only process — which never registers the task — still applies
    // them when it publishes. There is no per-task priority option, so priority
    // stays undefined and falls back to the queue default in the core.
    const options = this.tasks.get(subscription.taskName)?.options;
    return this.native.registerSubscription(
      subscription.topic,
      subscription.subscriptionName,
      subscription.taskName,
      subscription.queue,
      subscription.durable,
      ownerWorkerId,
      undefined,
      options?.maxRetries,
      options?.timeoutMs,
    );
  }

  /**
   * Run enqueue interceptors, merge per-task defaults, run the middleware
   * `onEnqueue` hooks, then serialize the args and encode the options — the
   * shared path for {@link Queue.enqueue} and {@link Queue.enqueueMany}.
   * Everything downstream of the interceptors keys off the (possibly
   * redirected) final task name.
   */
  private prepareEnqueue<Name extends keyof TTasks & string>(
    name: Name,
    args: Parameters<TTasks[Name]> | undefined,
    options: EnqueueOptions | undefined,
    mode?: { batch: boolean },
  ): { taskName: string; payload: Buffer; options: NativeEnqueueOptions } {
    const { taskName, args: finalArgs } = this.runInterceptors(name, [...(args ?? [])], mode);
    const defaults = this.tasks.get(taskName)?.options;
    const merged: EnqueueOptions = {
      ...options,
      maxRetries: options?.maxRetries ?? defaults?.maxRetries,
      timeoutMs: options?.timeoutMs ?? defaults?.timeoutMs,
    };
    // Middleware seam: let onEnqueue hooks validate/redact/rewrite before serializing.
    const ctx: EnqueueContext = { taskName, args: finalArgs, options: merged };
    for (const mw of this.middleware) {
      mw.onEnqueue?.(ctx);
    }
    // Gate: predicates see the (possibly rewritten) args and may reject the enqueue.
    for (const gate of this.gates.get(taskName) ?? []) {
      if (!gate({ taskName, args: ctx.args })) {
        this.emitter.emit("predicate.rejected", { taskName });
        throw new PredicateRejectedError(taskName);
      }
    }
    return {
      taskName,
      payload: this.encodeTaskPayload(taskName, ctx.args),
      options: toNativeEnqueueOptions(ctx.options),
    };
  }

  /**
   * Chain the registered interceptors over `(taskName, args)`: a null-ish
   * outcome or `reject` throws; `redirect` swaps both
   * the task name and args, and is rejected for batch enqueue (the batch is
   * stored under one task name) and for tasks with per-task codecs (the
   * redirect target's codec chain cannot be resolved from a bare name —
   * a cross-SDK behavioral contract).
   */
  private runInterceptors(
    name: string,
    args: unknown[],
    mode?: { batch: boolean },
  ): { taskName: string; args: unknown[] } {
    const outcomes: Interception[] = [];
    const startedAt = this.interceptors.length > 0 ? performance.now() : 0;
    try {
      return this.applyInterceptors(name, args, mode, outcomes);
    } finally {
      if (this.interceptors.length > 0) {
        this.interceptionMetrics.record(outcomes, performance.now() - startedAt);
      }
    }
  }

  private applyInterceptors(
    name: string,
    args: unknown[],
    mode: { batch: boolean } | undefined,
    outcomes: Interception[],
  ): { taskName: string; args: unknown[] } {
    let taskName = name;
    let currentArgs = args;
    for (const interceptor of this.interceptors) {
      const outcome = interceptor(taskName, currentArgs);
      if (!outcome) {
        throw new InterceptionError(`interceptor returned null for task "${taskName}"`);
      }
      outcomes.push(outcome);
      switch (outcome.type) {
        case "pass":
          break;
        case "convert":
          currentArgs = outcome.args;
          break;
        case "redirect":
          if (mode?.batch) {
            throw new InterceptionError(
              `interceptor Redirect is not supported for batch enqueue of task "${taskName}"`,
            );
          }
          taskName = outcome.taskName;
          currentArgs = outcome.args;
          break;
        case "reject":
          throw new InterceptionError(`enqueue of "${taskName}" rejected: ${outcome.reason}`);
      }
    }
    if (taskName !== name && (this.tasks.get(name)?.options?.codecs?.length ?? 0) > 0) {
      throw new InterceptionError(
        `interceptor Redirect is not supported for a task with payload codecs ("${name}")`,
      );
    }
    return { taskName, args: currentArgs };
  }

  /**
   * Serialize task args (call-shaped, honoring wire serializers) and apply
   * the task's named codecs in order. Payload only — results stay on the
   * queue serializer's plain `serialize`.
   */
  private encodeTaskPayload(taskName: string, args: unknown): Buffer {
    let data = serializeCall(this.serializer, Array.isArray(args) ? args : [args]);
    for (const name of this.tasks.get(taskName)?.options?.codecs ?? []) {
      const codec = this.codecs.get(name);
      if (!codec) {
        throw new SerializationError(`no codec registered named "${name}"`);
      }
      data = codec.encode(data);
    }
    return Buffer.from(data);
  }

  /** Fetch a job by id, or `null` if unknown. */
  getJob(id: string): Job | null {
    return this.native.getJob(id);
  }

  /** Deserialized result of a completed job, or `undefined` if not yet ready. */
  getResult(id: string): unknown {
    const job = this.native.getJob(id);
    if (!job?.result) {
      return undefined;
    }
    return this.serializer.deserialize(job.result);
  }

  /** Cancel a pending job. Returns false if it was not pending. */
  cancelJob(id: string): boolean {
    return this.native.cancelJob(id);
  }

  /** Request cooperative cancellation of a running job. Returns false if it is not running. */
  requestCancel(id: string): boolean {
    return this.native.requestCancel(id);
  }

  /** Whether cancellation has been requested for a job. */
  isCancelRequested(id: string): boolean {
    return this.native.isCancelRequested(id);
  }

  /**
   * Await a job's terminal state and return its deserialized result. Rejects
   * with {@link JobFailedError} / {@link JobCancelledError} on failure, and with
   * {@link TaskitoError} if the wait times out.
   */
  async result(id: string, options?: ResultOptions): Promise<unknown> {
    const timeoutMs = options?.timeoutMs ?? 30_000;
    const pollMs = options?.pollMs ?? 50;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const job = this.native.getJob(id);
      if (job) {
        switch (job.status) {
          case "complete":
            return job.result ? this.serializer.deserialize(job.result) : undefined;
          case "failed":
          case "dead":
            throw new JobFailedError(id, job.error ?? "job failed");
          case "cancelled":
            throw new JobCancelledError(id);
        }
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
    throw new ResultTimeoutError(id, timeoutMs);
  }

  /**
   * Async-iterate the partial results a job publishes via `currentJob().publish()`,
   * in order, until the job terminates (or the timeout elapses). Each value is the
   * JSON-deserialized argument passed to `publish`.
   */
  async *stream(id: string, options?: StreamOptions): AsyncIterableIterator<unknown> {
    const timeoutMs = options?.timeoutMs ?? 60_000;
    const pollMs = options?.pollMs ?? 200;
    const deadline = Date.now() + timeoutMs;
    // Cursor-based: each poll fetches only rows after the last seen log id
    // (UUIDv7 → time-ordered), instead of rescanning the full history.
    let cursor: string | undefined;
    for (;;) {
      const batch = this.newPartials(id, cursor);
      cursor = batch.cursor;
      yield* batch.values;
      const job = this.native.getJob(id);
      if (job && TERMINAL_STATUSES.has(job.status)) {
        yield* this.newPartials(id, cursor).values; // drain values committed at completion
        return;
      }
      if (Date.now() >= deadline) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  }

  /** Raw task-log entries for a job (oldest first), including published partials. */
  taskLogs(id: string): TaskLog[] {
    return this.native.getTaskLogs(id);
  }

  /**
   * Task logs across jobs, newest first, filtered by task name and/or level.
   * `sinceMs` is a Unix-ms lower bound (default: the last hour).
   */
  queryLogs(
    options: { task?: string; level?: TaskLogLevelFilter; sinceMs?: number; limit?: number } = {},
  ): Promise<TaskLog[]> {
    const sinceMs = options.sinceMs ?? Date.now() - 3_600_000;
    return this.native.queryTaskLogs(options.task, options.level, sinceMs, options.limit ?? 100);
  }

  /** Circuit-breaker state for every task that has one. */
  listCircuitBreakers(): Promise<CircuitBreaker[]> {
    return this.native.listCircuitBreakers();
  }

  /** Re-enqueue a copy of a job and record it in the replay history. Returns the new job id. */
  replay(id: string): Promise<string> {
    return this.native.replayJob(id);
  }

  /** Replays recorded for a job, newest first. */
  replayHistory(id: string): Promise<ReplayEntry[]> {
    return this.native.getReplayHistory(id);
  }

  /** The dependency DAG reachable from a job (nodes + dependency->dependent edges). */
  jobDag(id: string): Promise<JobDag> {
    return this.native.jobDag(id);
  }

  /** Partial-result values logged after `cursor`, plus the advanced cursor. */
  private newPartials(id: string, cursor?: string): { values: unknown[]; cursor?: string } {
    const logs = this.native.getTaskLogsAfter(id, cursor);
    return {
      values: logs
        .filter((log) => log.level === STREAM_LEVEL)
        .map((log) => decodePartial(log.extra)),
      cursor: logs[logs.length - 1]?.id ?? cursor,
    };
  }

  /** Job counts by status across all queues. */
  stats(): Promise<Stats> {
    return this.native.stats();
  }

  /** Job counts by status for a single queue. */
  statsByQueue(queue: string): Promise<Stats> {
    return this.native.statsByQueue(queue);
  }

  /**
   * Count pending jobs on `queue` — the lean primitive behind the `maxPending`
   * admission cap (avoids the full {@link Queue.statsByQueue} breakdown).
   */
  countPendingByQueue(queue: string): number {
    return this.native.countPendingByQueue(queue);
  }

  /** Job counts by status, keyed by queue name. */
  statsAllQueues(): Promise<Record<string, Stats>> {
    return this.native.statsAllQueues();
  }

  /** List jobs, optionally filtered and paginated. */
  listJobs(filter?: JobFilter): Promise<Job[]> {
    return this.native.listJobs(filter);
  }

  /**
   * List jobs on the wider filter: everything {@link Queue.listJobs} matches on,
   * plus metadata/error substrings and a created-at range.
   */
  listJobsFiltered(filter?: DetailedJobFilter): Promise<Job[]> {
    return this.native.listJobsFiltered(filter);
  }

  /** List archived (completed and moved out of the live table) jobs, newest first. */
  listArchived(limit?: number, offset?: number): Promise<Job[]> {
    return this.native.listArchived(limit, offset);
  }

  /**
   * Keyset-paginated {@link Queue.listJobs}, ordered by created time. Pass a
   * page's `nextCursor` back as `after`; `null` means the last page.
   *
   * O(page) at any depth on SQLite/Postgres. On Redis the status indexes are not
   * seekable, so the keyset is applied in memory — correct, but O(matching rows).
   */
  async listJobsAfter(filter?: CursorJobFilter, after?: string): Promise<Page<Job>> {
    return toPage(await this.native.listJobsAfter(filter, after));
  }

  /**
   * Keyset-paginated {@link Queue.listJobsFiltered}, ordered by created time.
   * See {@link Queue.listJobsAfter} for the cursor contract.
   */
  async listJobsFilteredAfter(
    filter?: CursorDetailedJobFilter,
    after?: string,
  ): Promise<Page<Job>> {
    return toPage(await this.native.listJobsFilteredAfter(filter, after));
  }

  /**
   * Keyset-paginated {@link Queue.listArchived}, ordered by completed time.
   * See {@link Queue.listJobsAfter} for the cursor contract.
   */
  async listArchivedAfter(limit?: number, after?: string): Promise<Page<Job>> {
    return toPage(await this.native.listArchivedAfter(limit, after));
  }

  /**
   * Keyset-paginated {@link Queue.deadLetters}, ordered by failed time.
   * See {@link Queue.listJobsAfter} for the cursor contract.
   */
  async deadLettersAfter(limit?: number, after?: string): Promise<Page<DeadJob>> {
    return toPage(await this.native.deadLettersAfter(limit, after));
  }

  /** Error history for a job (one entry per failed attempt). */
  getJobErrors(id: string): Promise<JobError[]> {
    return this.native.getJobErrors(id);
  }

  /** Per-execution task metrics recorded at or after `sinceMs` (Unix epoch ms). */
  getMetrics(sinceMs: number, task?: string): Promise<Metric[]> {
    return this.native.getMetrics(task ?? null, sinceMs);
  }

  /** List dead-letter entries (paginated). */
  deadLetters(limit?: number, offset?: number): Promise<DeadJob[]> {
    return this.native.deadLetters(limit, offset);
  }

  /** List dead-letter entries for a single task (paginated, newest first). */
  deadLettersByTask(taskName: string, limit?: number, offset?: number): Promise<DeadJob[]> {
    return this.native.deadLettersByTask(taskName, limit, offset);
  }

  /** Delete every dead-letter entry for a task. Returns the count removed. */
  purgeDeadByTask(taskName: string): Promise<number> {
    return this.native.purgeDeadByTask(taskName);
  }

  /** Re-enqueue a dead-letter entry. Returns the new job id. */
  retryDead(deadId: string): string {
    return this.native.retryDead(deadId);
  }

  /** Delete a dead-letter entry. Returns false if it didn't exist. */
  deleteDead(deadId: string): boolean {
    return this.native.deleteDead(deadId);
  }

  /** Purge dead-letter entries older than `olderThanMs`. Returns the count removed. */
  purgeDead(olderThanMs: number): Promise<number> {
    return this.native.purgeDead(olderThanMs);
  }

  /** Purge completed jobs older than `olderThanMs`. Returns the count removed. */
  purgeCompleted(olderThanMs: number): Promise<number> {
    return this.native.purgeCompleted(olderThanMs);
  }

  /** Pause a queue — workers stop dispatching its jobs until resumed. */
  pauseQueue(queue: string): void {
    this.native.pauseQueue(queue);
    this.emitter.emit("queue.paused", { queue });
  }

  /** Resume a paused queue. */
  resumeQueue(queue: string): void {
    this.native.resumeQueue(queue);
    this.emitter.emit("queue.resumed", { queue });
  }

  /** Names of currently-paused queues. */
  listPausedQueues(): string[] {
    return this.native.listPausedQueues();
  }

  /** Read a dashboard settings key, or `null` when unset. */
  getSetting(key: string): string | null {
    return this.native.getSetting(key);
  }

  /** Write a dashboard settings key. */
  setSetting(key: string, value: string): void {
    this.native.setSetting(key, value);
  }

  /** Delete a dashboard settings key. Returns false if it didn't exist. */
  deleteSetting(key: string): boolean {
    return this.native.deleteSetting(key);
  }

  /** All dashboard settings as a key → value record. */
  listSettings(): Record<string, string> {
    return this.native.listSettings();
  }

  /**
   * The retention windows a worker is applying to this namespace, or `null`
   * when no worker has swept yet — distinct from retention being disabled,
   * which reports with `enabled: false`.
   */
  effectiveRetention(): EffectiveRetention | null {
    const raw = this.native.effectiveRetention();
    return raw === null ? null : parseEffectiveRetention(raw);
  }

  /**
   * Preview what a retention purge would delete right now, without deleting
   * anything. With no argument the preview follows the policy the elected
   * cleaner reported for this namespace (recommended defaults only when no
   * cleaner has swept yet); pass candidate `retention` windows to size a
   * window before setting it — no worker reconfiguration needed. The counts
   * are a point-in-time snapshot; nothing is deleted.
   */
  async dryRunRetention(retention?: RetentionOptions): Promise<RetentionPreview> {
    return parseRetentionPreview(await this.native.dryRunRetention(retention));
  }

  // ── Task & queue overrides (dashboard-tunable runtime config) ─────

  /** Every persisted task override keyed by task name. */
  listTaskOverrides(): Map<string, TaskOverride> {
    return new OverridesStore(this.native).listTasks();
  }

  getTaskOverride(taskName: string): TaskOverride | undefined {
    return new OverridesStore(this.native).getTask(taskName);
  }

  /**
   * Set or update a task override; `null` clears a field. Allowed fields:
   * `rate_limit`, `max_concurrent`, `max_retries`, `retry_backoff`,
   * `timeout`, `priority`, `paused`. Applied on the next worker start.
   */
  setTaskOverride(taskName: string, fields: Record<string, unknown>): TaskOverride {
    return new OverridesStore(this.native).setTask(taskName, fields);
  }

  clearTaskOverride(taskName: string): boolean {
    return new OverridesStore(this.native).clearTask(taskName);
  }

  listQueueOverrides(): Map<string, QueueOverride> {
    return new OverridesStore(this.native).listQueues();
  }

  getQueueOverride(queueName: string): QueueOverride | undefined {
    return new OverridesStore(this.native).getQueue(queueName);
  }

  /** Allowed fields: `rate_limit`, `max_concurrent`, `paused`. */
  setQueueOverride(queueName: string, fields: Record<string, unknown>): QueueOverride {
    return new OverridesStore(this.native).setQueue(queueName, fields);
  }

  clearQueueOverride(queueName: string): boolean {
    return new OverridesStore(this.native).clearQueue(queueName);
  }

  /**
   * Every registered task with its registration defaults, any active
   * override, and the effective values for the next worker start
   * (snake_case, dashboard contract; durations in seconds).
   */
  registeredTasks(): Array<Record<string, unknown>> {
    const overrides = this.listTaskOverrides();
    const out: Array<Record<string, unknown>> = [];
    for (const [name, task] of this.tasks) {
      const options = task.options ?? {};
      const defaults: Record<string, unknown> = {
        max_retries: options.maxRetries ?? null,
        retry_backoff:
          options.retryBackoff?.baseMs !== undefined ? options.retryBackoff.baseMs / 1000 : null,
        timeout: options.timeoutMs !== undefined ? options.timeoutMs / 1000 : null,
        priority: null,
        rate_limit: options.rateLimit ?? null,
        max_concurrent: options.maxConcurrent ?? null,
      };
      const override = overrides.get(name);
      const patch = overridePatch(override);
      out.push({
        name,
        queue: "default",
        defaults,
        override: override ? { ...patch, ...(override.paused ? { paused: true } : {}) } : null,
        effective: { ...defaults, ...patch },
        paused: override?.paused ?? false,
      });
    }
    return out;
  }

  /** Every known queue with its limits, override, and paused state. */
  registeredQueues(): Array<Record<string, unknown>> {
    const overrides = this.listQueueOverrides();
    const pausedSet = new Set(this.listPausedQueues());
    const names = new Set<string>(["default", ...this.queueLimits.keys(), ...overrides.keys()]);
    const out: Array<Record<string, unknown>> = [];
    for (const name of [...names].sort()) {
      const limits = this.queueLimits.get(name);
      const defaults: Record<string, unknown> = {};
      if (limits?.rateLimit !== undefined) {
        defaults.rate_limit = limits.rateLimit;
      }
      if (limits?.maxConcurrent !== undefined) {
        defaults.max_concurrent = limits.maxConcurrent;
      }
      const override = overrides.get(name);
      const patch = overridePatch(override);
      out.push({
        name,
        defaults,
        override: override ? { ...patch, ...(override.paused ? { paused: true } : {}) } : null,
        effective: { ...defaults, ...patch },
        paused: pausedSet.has(name) || (override?.paused ?? false),
      });
    }
    return out;
  }

  // ── Middleware admin (dashboard toggles) ──────────────────────────

  /** Every registered middleware with its name, class path, and scopes. */
  listMiddleware(): Array<Record<string, unknown>> {
    const seen = new Map<string, Record<string, unknown>>();
    this.middleware.forEach((mw, index) => {
      const name = middlewareKey(mw, index);
      if (!seen.has(name)) {
        seen.set(name, {
          name,
          class_path: mw.constructor?.name ?? "Object",
          scopes: [{ kind: "global" }],
        });
      }
    });
    return [...seen.values()];
  }

  /** Every task with at least one disabled middleware. */
  listMiddlewareDisables(): Record<string, string[]> {
    return new MiddlewareDisableStore(this.native).listAll();
  }

  getDisabledMiddlewareFor(taskName: string): string[] {
    return new MiddlewareDisableStore(this.native).getFor(taskName);
  }

  /** Disable one middleware for one task (takes effect on the next job). */
  disableMiddlewareForTask(taskName: string, middlewareName: string): string[] {
    return new MiddlewareDisableStore(this.native).setDisabled(taskName, middlewareName, true);
  }

  enableMiddlewareForTask(taskName: string, middlewareName: string): string[] {
    return new MiddlewareDisableStore(this.native).setDisabled(taskName, middlewareName, false);
  }

  /** Clear ALL disables for a task — every middleware fires again. */
  clearMiddlewareDisables(taskName: string): boolean {
    return new MiddlewareDisableStore(this.native).clearFor(taskName);
  }

  /** Per-handler proxy reconstruction metrics for this process. */
  proxyStats(): ProxyHandlerStats[] {
    return proxyMetrics.toList();
  }

  /** Enqueue-interception metrics for this process. */
  interceptionStats() {
    return this.interceptionMetrics.toDict();
  }

  /** Registered workers (heartbeat + identity). */
  listWorkers(): Promise<WorkerInfo[]> {
    return this.native.listWorkers();
  }

  /** Start a worker that runs the registered tasks. Hold the returned {@link Worker}. */
  runWorker(options?: WorkerRunOptions): Worker {
    return Worker.start(this.native, {
      tasks: this.tasks,
      queueLimits: this.queueLimits,
      serializer: this.serializer,
      codecs: this.codecs,
      middleware: this.middleware,
      emitter: this.emitter,
      resources: this.resources,
      workflowTracker: this.trackerIfSupported(),
      declareSubscriptions: (workerId) => this.declareWorkerSubscriptions(workerId),
      logConsumers: this.pendingLogConsumers,
      run: options,
    });
  }
}

/** A subscription recorded by {@link Queue.subscriber}, pending storage registration. */
interface PendingSubscription {
  topic: string;
  subscriptionName: string;
  taskName: string;
  queue: string;
  durable: boolean;
}

/** Log level used for published partial results (matches the cross-SDK contract). */
const STREAM_LEVEL: TaskLogLevel = "result";
/** Job statuses at which a stream stops. */
const TERMINAL_STATUSES = new Set(["complete", "failed", "dead", "cancelled"]);

/** Decode a partial-result log's `extra` (JSON, falling back to the raw string). */
function decodePartial(extra: string | null | undefined): unknown {
  if (!extra) {
    return undefined;
  }
  try {
    return JSON.parse(extra);
  } catch {
    return extra;
  }
}

/**
 * Convert public enqueue options to the native shape: structured `notes` is
 * validated and encoded to canonical JSON; all other fields pass through.
 */
function toNativeEnqueueOptions(options: EnqueueOptions): NativeEnqueueOptions {
  const { notes, ...rest } = options;
  return notes === undefined ? rest : { ...rest, notes: encodeNotes(notes) };
}

/** Default on-disk SQLite location — mirrors the Python SDK's `.taskito/taskito.db`. */
const DEFAULT_SQLITE_DB = ".taskito/taskito.db";

/** Resolve a {@link QueueOptions} into the native open options. */
function toOpenOptions(options: QueueOptions): OpenOptions {
  const backend = options.backend ?? "sqlite";
  if (backend === "sqlite") {
    // Zero-config default, like Python: an on-disk DB under `.taskito/`.
    const dsn = options.dsn ?? options.dbPath ?? DEFAULT_SQLITE_DB;
    ensureSqliteParentDir(dsn);
    return { backend, dsn, poolSize: options.poolSize, namespace: options.namespace };
  }
  // Postgres/Redis have no sensible default endpoint — require an explicit dsn.
  // The Postgres `schema` (default `"taskito"`, resolved in the addon) and the
  // Redis `prefix` give each backend its own isolated namespace.
  const dsn = options.dsn;
  if (!dsn) {
    throw new QueueError(`Queue backend "${backend}" requires a \`dsn\` connection string`);
  }
  return {
    backend,
    dsn,
    poolSize: options.poolSize,
    schema: options.schema,
    prefix: options.prefix,
    namespace: options.namespace,
  };
}

/** Non-null override fields, minus identity/bookkeeping ones (contract patch shape). */
function overridePatch(
  override: TaskOverride | QueueOverride | undefined,
): Record<string, unknown> {
  if (!override) {
    return {};
  }
  const patch: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(override)) {
    if (key === "task_name" || key === "queue_name" || key === "updated_at" || key === "paused") {
      continue;
    }
    if (value !== null) {
      patch[key] = value;
    }
  }
  return patch;
}

/**
 * Create the parent directory of a SQLite file path, as the Python SDK does —
 * SQLite won't create missing directories itself. In-memory databases have no
 * parent and are skipped.
 */
function ensureSqliteParentDir(dsn: string): void {
  if (dsn === ":memory:" || dsn.startsWith("file::memory:")) {
    return;
  }
  const dir = dirname(dsn);
  if (dir && dir !== ".") {
    mkdirSync(dir, { recursive: true });
  }
}
