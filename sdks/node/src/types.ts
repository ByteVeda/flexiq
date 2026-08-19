import type { DebounceOptions } from "./debounce";
import type {
  CircuitBreakerInput,
  DetailedJobFilter,
  JobFilter,
  MeshWorkerConfig,
  EnqueueOptions as NativeEnqueueOptions,
  PublishOptions as NativePublishOptions,
} from "./native";

export type {
  CircuitBreakerInput as CircuitBreakerOptions,
  DetailedJobFilter,
  JobFilter,
  JsCircuitBreaker as CircuitBreaker,
  JsDagEdge as DagEdge,
  JsDeadJob as DeadJob,
  JsJob as Job,
  JsJobDag as JobDag,
  JsJobError as JobError,
  JsMetric as Metric,
  JsReplayEntry as ReplayEntry,
  JsStats as Stats,
  JsSubscription as Subscription,
  JsTaskLog as TaskLog,
  JsTopic as DeclaredTopic,
  JsTopicLogStat as TopicLogStat,
  JsTopicStat as TopicStat,
  JsWorkerRow as WorkerInfo,
  MeshWorkerConfig,
} from "./native";

/**
 * Same-priority dispatch order. `"lifo"` runs newest-first under overload (a
 * freshness lever); `"fifo"` (default) is the fair oldest-first ordering.
 */
export type DispatchOrder = "fifo" | "lifo";

/** Every dispatch order, for runtime validation. */
export const DISPATCH_ORDERS: readonly DispatchOrder[] = ["fifo", "lifo"];

/**
 * Severity of a structured task log. `"result"` is not a severity — it carries
 * partial results published from a running task. Distinct from the SDK's own
 * logger level (`LogLevel` in `./utils`): this one is persisted and read back
 * by every SDK.
 */
export type TaskLogLevel = "debug" | "info" | "warning" | "error" | "critical" | "result";

/**
 * A task-log level as a *filter*: any string is accepted, because an unrecognized
 * value must filter to nothing rather than fail — filters usually arrive from a
 * dashboard query string. The union still autocompletes the known levels.
 */
export type TaskLogLevelFilter = TaskLogLevel | (string & {});

/** Every task-log level, for runtime validation. */
export const TASK_LOG_LEVELS: readonly TaskLogLevel[] = [
  "debug",
  "info",
  "warning",
  "error",
  "critical",
  "result",
];

/** One message pulled from a log topic; `id` is the cursor token for `ackTopic`. */
export interface TopicMessage {
  /** Message id — pass to `ackTopic` to advance the cursor past it. */
  id: string;
  /** Deserialized positional args from the `publish` call. */
  args: unknown[];
  /** Caller metadata, if any. */
  metadata?: Record<string, unknown>;
  /** Structured notes, if any. */
  notes?: Record<string, unknown>;
  /** Unix-millisecond publish time. */
  createdAt: number;
}

/**
 * Per-job enqueue options. Mirrors the native options, but `notes` is a
 * structured object here (validated and JSON-encoded before it reaches the
 * core) rather than a pre-encoded string, and the debounce fields are the
 * public spelling — the millisecond native ones are derived from them.
 */
export interface EnqueueOptions
  extends Omit<NativeEnqueueOptions, "notes" | NativeDebounceField>,
    DebounceInput {
  /** Structured annotations stored on the job — at most 15 fields, 4 KiB encoded. */
  notes?: Record<string, unknown>;
}

/** The wire spelling of the debounce fields, replaced by {@link DebounceInput}. */
type NativeDebounceField =
  | "debounceKey"
  | "debounceWindowMs"
  | "debounceMaxWaitMs"
  | "debounceReplacePayload";

/**
 * Per-publish options. Mirrors the native options, but `notes` is a structured
 * object here (validated and JSON-encoded before it reaches the core) rather
 * than a pre-encoded string.
 */
export interface PublishOptions extends Omit<NativePublishOptions, "notes"> {
  /** Structured annotations stamped on every delivery (plus `topic`/`subscription`). */
  notes?: Record<string, unknown>;
}

/** Options for {@link Queue.subscriber}: task options plus subscription routing.
 *  `codecs` is not supported — published payloads use the queue-level serializer. */
export interface SubscriberOptions extends TaskOptions {
  /** Stable subscription identity — re-registering the same `(topic, name)`
   *  updates the routing target instead of duplicating. Defaults to the task name. */
  subscriptionName?: string;
  /** Queue the subscriber's delivery jobs go to (default `"default"`). */
  queue?: string;
  /** Persist across restarts (default true). `false` = ephemeral: registered
   *  only by a running worker and reaped once that worker stops heartbeating. */
  durable?: boolean;
}

/** Options for {@link Queue.logConsumer}: a managed pull loop over a log topic. */
export interface LogConsumerOptions {
  /** Milliseconds to wait after an empty poll before re-reading (default 1000). */
  pollIntervalMs?: number;
  /** Max messages pulled per poll (default 100). */
  batchSize?: number;
  /** `"retry"` (default) leaves a failed message un-acked so the batch re-reads;
   *  `"skip"` acks past it and continues. */
  onError?: "retry" | "skip";
}

/** Options for {@link Queue.result}. */
export interface ResultOptions {
  /** Max time to wait for a terminal state (ms). Default 30000. */
  timeoutMs?: number;
  /** Poll interval (ms). Default 50. */
  pollMs?: number;
}

/** Options for {@link Queue.stream}. */
export interface StreamOptions {
  /** Max time to wait for the job to terminate (ms). Default 60000. */
  timeoutMs?: number;
  /** Poll interval (ms). Default 200. */
  pollMs?: number;
}

/**
 * A registered task: receives the deserialized positional args and returns a
 * (possibly async) result.
 */
export type TaskHandler<Args extends unknown[] = unknown[], Result = unknown> = (
  ...args: Args
) => Result | Promise<Result>;

/** A heterogeneous task handler, as stored in the registry. */
// biome-ignore lint/suspicious/noExplicitAny: registry handlers have varied signatures
export type AnyHandler = (...args: any[]) => any;

/** Task name -> handler signature, accumulated by chaining {@link Queue.task}. */
export type TaskMap = Record<string, AnyHandler>;

/** A rate-limit spec: `<count>/<unit>` with unit `s | m | h` (e.g. `"100/m"`). */
export type RateLimit = `${number}/${"s" | "m" | "h"}`;

/** What a saturated rate limit does to a job: keep it, or shed it. */
export type OnExcess = "defer" | "drop";

/**
 * A length of time: milliseconds as a number, or a suffixed string —
 * `"500ms"`, `"30s"`, `"5m"`, `"2h"`, `"1d"`.
 */
export type Duration = number | `${number}${"ms" | "s" | "m" | "h" | "d"}`;

/**
 * Debounce fields, shared by {@link TaskOptions} (the task's default) and
 * {@link EnqueueOptions} (a per-call override). Debouncing collapses a burst of
 * enqueues that share a key into one run whose deadline slides forward with
 * each call — distinct from `uniqueKey`, which dedups onto the first job and
 * never moves it.
 */
export interface DebounceInput {
  /**
   * Collapse window: each enqueue pushes the run this far into the future.
   * Setting it is what turns debouncing on, and requires both
   * {@link DebounceInput.debounceKey} and {@link DebounceInput.debounceMaxWait}.
   */
  debounce?: Duration;
  /**
   * The window's identity, as a template resolved against the call's args:
   * `{userId}` reads that property off the first object argument carrying it,
   * `{0}`/`{1}` read an argument by position. A placeholder that resolves to
   * nothing throws at enqueue rather than degrading to a key shared by every
   * caller. A template with no placeholder is a single window for the task.
   */
  debounceKey?: string;
  /**
   * Ceiling on the total delay, measured from when the window opened.
   * Mandatory: without it a caller who never stops enqueuing starves the job.
   */
  debounceMaxWait?: Duration;
  /**
   * Whether a repeat enqueue overwrites the pending job's payload with its own.
   * Default false — the job runs with the args the window opened with.
   */
  debounceReplacePayload?: boolean;
}

/** Per-task defaults and resilience config, applied when registering a task. */
export interface TaskOptions extends DebounceInput {
  /** Retry budget (also the per-job default at enqueue). */
  maxRetries?: number;
  /** Exponential backoff bounds for retries. */
  retryBackoff?: { baseMs?: number; maxMs?: number };
  /**
   * Classifies a thrown error as retryable. Returning `false` dead-letters the
   * job immediately, whatever retry budget is left — use it for permanent
   * failures (a malformed payload, a 4xx) that no amount of retrying fixes.
   * Unset retries everything, as does a predicate that throws.
   *
   * Synchronous by design: the job cannot settle until it answers. It sees
   * every error raised while running the task, not only the handler's: a
   * `before`/`after` middleware hook or result serialization can fail too, so a
   * whitelist predicate dead-letters those as well. Payload decoding fails
   * earlier and always retries, as does a timeout (detected outside the
   * handler).
   */
  retryOn?: (error: unknown) => boolean;
  /** Per-job timeout default (ms); enforced by the worker. */
  timeoutMs?: number;
  /** Cap on concurrently-running jobs of this task, across the cluster. */
  maxConcurrent?: number;
  /**
   * Cap on this task's share of a single worker's dispatch slots, so one slow
   * task cannot occupy the whole pool and starve the others. In-process and
   * free, unlike {@link TaskOptions.maxConcurrent}, which is cluster-wide and
   * costs a database read.
   */
  maxInFlightPerTask?: number;
  /** Rate-limit spec like `"100/m"`, `"50/s"`, `"3600/h"`. */
  rateLimit?: RateLimit;
  /**
   * What a saturated rate limit does to this task's jobs. `"defer"` (the
   * default) reschedules the job for a later dispatch cycle. `"drop"` sheds it:
   * the job is dead-lettered on the spot with a reserved `rate_limit:` reason,
   * so shedding stays visible in the dashboard and countable in metrics, and
   * the dead-letter auto-retry sweep never resurrects it. Suits traffic whose
   * value expires with the moment — metrics samples, cache warms — where a
   * backlog is worth less than nothing.
   *
   * Applies to the limit on this task **and** to the one on the queue it runs
   * in, since either rejecting means the same thing to the caller. A tripped
   * {@link TaskOptions.circuitBreaker} always defers regardless: that is
   * downstream failure, not excess load. Dropping fires no middleware or event
   * hooks — the job never ran.
   */
  onExcess?: OnExcess;
  /**
   * Cap on how fast this task may **retry**, across all of its jobs — same spec
   * as {@link TaskOptions.rateLimit}. Once spent, failures dead-letter instead
   * of retrying, so a broken dependency cannot become a retry storm. Distinct
   * from {@link TaskOptions.maxRetries}, which bounds one job rather than the
   * rate, and from {@link TaskOptions.circuitBreaker}, which trips on hard
   * failure rather than aggregate retry rate.
   */
  retryBudget?: RateLimit;
  /** Trip the task's circuit breaker after repeated failures. */
  circuitBreaker?: CircuitBreakerInput;
  /**
   * Resource names injected as a trailing `deps` object: the handler is called
   * `handler(...args, deps)`. Use the {@link Queue.task} `inject` overload so the
   * `deps` param is stripped from the typed `enqueue` args.
   */
  inject?: readonly string[];
  /**
   * Names of payload codecs (registered via `QueueOptions.codecs`) applied in
   * order to this task's serialized payload on enqueue and reversed on the
   * worker. Payload only — results stay on the queue serializer.
   */
  codecs?: readonly string[];
}

/** Options for {@link Queue.discover}. */
export interface DiscoverOptions {
  /**
   * File extensions to import, replacing the default set (`.js`, `.mjs`, `.cjs`,
   * `.ts`, `.mts`, `.cts`). The leading dot is optional — `"mjs"` and `".mjs"`
   * mean the same thing. Declaration files (`.d.ts` and friends) are never
   * imported.
   *
   * Narrow this when a directory holds sources next to their compiled output:
   * importing both `x.ts` and `x.js` declares every task in them twice, which is
   * a {@link DuplicateTaskError}.
   */
  extensions?: readonly string[];
}

/** Options for {@link Queue.registerPeriodic}. */
export interface PeriodicOptions {
  /** Positional args passed to the task each time it fires. */
  args?: unknown[];
  /** Queue the periodic job runs on (default `"default"`). */
  queue?: string;
  /** IANA timezone for the cron schedule (default UTC). */
  timezone?: string;
  /** Register disabled (won't fire until re-registered enabled). Default true. */
  enabled?: boolean;
}

/** A registered periodic task, as returned by {@link Queue.listPeriodic}. Timestamps are Unix ms. */
export interface PeriodicTask {
  name: string;
  taskName: string;
  cronExpr: string;
  queue: string;
  enabled: boolean;
  /** Last fire time, or absent if it has not run yet. */
  lastRun?: number;
  nextRun: number;
  /** IANA timezone the cron is evaluated in, or absent for UTC. */
  timezone?: string;
}

/** Per-queue resilience config. */
export interface QueueLimits {
  maxConcurrent?: number;
  rateLimit?: RateLimit;
  /**
   * Opt-in admission cap on the queue's pending backlog. Once reached,
   * `enqueue`/`enqueueMany` throw {@link QueueFullError}. Enforced producer-side
   * (a non-atomic count-then-insert), so it applies even with no worker running.
   */
  maxPending?: number;
  /**
   * Opt-in CoDel load shedding. Under sustained overload — a job's wait past its
   * eligibility staying above `targetMs` for a full `intervalMs` — the
   * dispatcher sheds the stalest jobs to the DLQ (reason prefixed `codel:`)
   * rather than running them stale. A transient spike is never shed.
   */
  codel?: { targetMs: number; intervalMs: number };
  /**
   * Same-priority dispatch order. `"lifo"` runs newest-first under overload (a
   * freshness lever); `"fifo"` (default) is the fair oldest-first ordering.
   * Priority always dominates. Honored on SQLite/Postgres; Redis is FIFO-only.
   */
  dispatchOrder?: DispatchOrder;
}

/** A task handler plus its registration options. */
export interface RegisteredTask {
  handler: AnyHandler;
  options?: TaskOptions;
  /** The validated debounce policy, built once at registration. */
  debounce?: DebounceOptions;
}

/**
 * One page of a keyset-paginated listing.
 *
 * `nextCursor` is `null` on the last page — a short page means the listing is
 * exhausted — so a walk is `while (cursor !== null)`. Pass it back verbatim as
 * the next call's `after`; treat it as opaque.
 */
export interface Page<T> {
  items: T[];
  nextCursor: string | null;
}

/**
 * A filter for a cursor-paginated listing: the offset-paged filter without its
 * `offset`. The two ways of paging do not compose — a cursor already says where
 * to resume — so the field is removed rather than silently ignored.
 */
export type CursorJobFilter = Omit<JobFilter, "offset">;

/** {@link DetailedJobFilter} without its `offset`. See {@link CursorJobFilter}. */
export type CursorDetailedJobFilter = Omit<DetailedJobFilter, "offset">;

/** Options for {@link Queue.runWorker}. */
export interface WorkerRunOptions {
  /** Queues to consume (default `["default"]`). */
  queues?: string[];
  /**
   * Buffer size for job hand-offs, and the capacity this worker advertises
   * (default 128). This does **not** bound how many jobs run at once — see
   * {@link WorkerRunOptions.concurrency}.
   */
  channelCapacity?: number;
  /**
   * Jobs this worker runs at once. Unset leaves it unbounded, so the worker can
   * claim more than it can run, stranding the surplus `running` while peers
   * sharing the database skip it. Set this on any worker sharing a database.
   */
  concurrency?: number;
  /** Jobs claimed per scheduler poll (default 1). */
  batchSize?: number;
  /**
   * Opt-in decentralized mesh overlay (peer gossip + work-stealing). Requires
   * the native addon to be built with the `mesh` cargo feature; ignored otherwise.
   */
  mesh?: MeshWorkerConfig;
  /**
   * Advance workflow runs as this worker's node-jobs settle (default true when
   * the addon supports workflows). Adds one job lookup per terminal job; set
   * false on workers that never process workflow steps.
   */
  advanceWorkflows?: boolean;
  /** Per-table retention windows for auto-cleanup. */
  retention?: RetentionOptions;
  /**
   * Opt into event-driven dispatch: an enqueue wakes the scheduler immediately
   * instead of it waiting for the next poll, removing the dispatch latency
   * floor and the idle database load of polling. Requires the native addon to
   * be built with the `push-dispatch` cargo feature; otherwise accepted and
   * ignored (polling is kept).
   */
  pushDispatch?: boolean;
  /**
   * Heartbeat cadence override, in ms (default 5000). Exists so tests can
   * exercise heartbeat-driven behavior quickly.
   *
   * @internal
   */
  heartbeatIntervalMs?: number;
}

/**
 * How long each history table keeps a row before auto-cleanup deletes it, in
 * seconds. An unset field keeps that table forever. A job or DLQ entry can
 * still carry its own per-entry `resultTtl`, honored independently.
 */
export interface RetentionOptions {
  /** Terminal jobs, every terminal status — the artifact read after completion. */
  archivedJobs?: number;
  /** Dead-letter entries — the only copy of a payload a human must act on. */
  deadLetter?: number;
  /** Task logs — highest write volume, lowest per-row value. */
  taskLogs?: number;
  /** Task metrics — feeds the dashboard charts. */
  taskMetrics?: number;
  /** Per-attempt job errors. */
  jobErrors?: number;
}

/**
 * The windows a worker is actually applying, as reported by the elected
 * cleaner on each sweep. Retention runs in the worker process, so this is the
 * policy that governs the deletes — not this process's configuration.
 * Windows are **milliseconds**; `null` keeps a table forever.
 */
export interface EffectiveRetention {
  /** False when no table has a window — only per-entry TTLs are swept. */
  enabled: boolean;
  /** True when the windows are the recommended defaults, set by no one. */
  defaulted: boolean;
  /** Namespace the windows cover. The purges are not queue-scoped. */
  namespace: string;
  /** When the cleaner last published this, in Unix milliseconds. */
  reportedAt: number;
  /** Per-table windows in milliseconds. */
  windows: {
    archivedJobs: number | null;
    deadLetter: number | null;
    taskLogs: number | null;
    taskMetrics: number | null;
    jobErrors: number | null;
  };
}

/**
 * What a retention purge would delete right now, without deleting anything.
 * Counts are a point-in-time snapshot taken at `referenceTime`, computed
 * against the previewed windows. Windows are **milliseconds**; `null` keeps a
 * table forever and its count reflects per-entry TTLs only.
 */
export interface RetentionPreview {
  /** False when no table has a window — only per-entry TTLs would be swept. */
  enabled: boolean;
  /** True when the windows are the recommended defaults, set by no one. */
  defaulted: boolean;
  /** Namespace the windows cover. The purges are not queue-scoped. */
  namespace: string;
  /** The `now` the snapshot was taken at, in Unix milliseconds. */
  referenceTime: number;
  /** Per-table windows in milliseconds; `null` keeps a table forever. */
  windows: {
    archivedJobs: number | null;
    deadLetter: number | null;
    taskLogs: number | null;
    taskMetrics: number | null;
    jobErrors: number | null;
  };
  /** Rows each table's purge would remove. */
  counts: {
    archivedJobs: number;
    deadLetter: number;
    taskLogs: number;
    taskMetrics: number;
    jobErrors: number;
  };
  /** Total rows a purge would delete across every table. */
  total: number;
}
