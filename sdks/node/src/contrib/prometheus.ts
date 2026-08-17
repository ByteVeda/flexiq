// Prometheus metrics for FlexiQ. Optional integration — import from
// `flexiq/contrib/prometheus`; requires `prom-client` as a peer.
//
// Register the middleware with `queue.use(prometheusMiddleware())` to record per-job
// counters/histograms, and run a `PrometheusStatsCollector` to poll queue depth / DLQ
// size. Expose `register.metrics()` from your HTTP server.

import { Counter, register as defaultRegister, Gauge, Histogram, type Registry } from "prom-client";
import type { Middleware } from "../middleware";
import type { Queue } from "../queue";
import { createLogger } from "../utils";

const log = createLogger("prometheus");

/** The per-(registry, namespace) metric bundle, created once and reused. */
interface FlexiQMetrics {
  jobsTotal: Counter<"task" | "status">;
  jobDuration: Histogram<"task">;
  activeWorkers: Gauge<string>;
  retriesTotal: Counter<"task">;
  queueDepth: Gauge<"queue">;
  dlqSize: Gauge<string>;
}

// prom-client throws on duplicate registration, so a namespace's metrics must be built
// exactly once per registry. Cache them — mirrors the Python contrib's `_metric_stores`.
const stores = new WeakMap<Registry, Map<string, FlexiQMetrics>>();

function getMetrics(register: Registry, namespace: string, buckets?: number[]): FlexiQMetrics {
  let byNamespace = stores.get(register);
  if (!byNamespace) {
    byNamespace = new Map();
    stores.set(register, byNamespace);
  }
  const cached = byNamespace.get(namespace);
  if (cached) {
    return cached;
  }
  const registers = [register];
  const metrics: FlexiQMetrics = {
    jobsTotal: new Counter({
      name: `${namespace}_jobs_total`,
      help: "Total finished jobs by task and outcome.",
      labelNames: ["task", "status"],
      registers,
    }),
    jobDuration: new Histogram({
      name: `${namespace}_job_duration_seconds`,
      help: "Task execution duration in seconds.",
      labelNames: ["task"],
      ...(buckets ? { buckets } : {}),
      registers,
    }),
    activeWorkers: new Gauge({
      name: `${namespace}_active_workers`,
      help: "Jobs currently executing.",
      registers,
    }),
    retriesTotal: new Counter({
      name: `${namespace}_retries_total`,
      help: "Total job retries by task.",
      labelNames: ["task"],
      registers,
    }),
    queueDepth: new Gauge({
      name: `${namespace}_queue_depth`,
      help: "Pending jobs per queue.",
      labelNames: ["queue"],
      registers,
    }),
    dlqSize: new Gauge({
      name: `${namespace}_dlq_size`,
      help: "Total jobs in the dead-letter queue.",
      registers,
    }),
  };
  byNamespace.set(namespace, metrics);
  return metrics;
}

/** Options shared by the middleware and the stats collector. */
interface CommonOptions {
  /** Metric name prefix (default `"flexiq"`). */
  namespace?: string;
  /** Registry to register on (default the prom-client global `register`). */
  register?: Registry;
}

/** Options for {@link prometheusMiddleware}. */
export interface PrometheusMiddlewareOptions extends CommonOptions {
  /** Only record tasks for which this returns true (default: all). */
  taskFilter?: (taskName: string) => boolean;
  /** Custom histogram buckets (seconds) for job duration. */
  buckets?: number[];
}

/**
 * Build {@link Middleware} that records job counters and duration histograms. Tracks
 * in-flight jobs as a gauge and retries as a counter.
 */
export function prometheusMiddleware(options: PrometheusMiddlewareOptions = {}): Middleware {
  const namespace = options.namespace ?? "flexiq";
  const register = options.register ?? defaultRegister;
  const metrics = getMetrics(register, namespace, options.buckets);
  const tracked = (taskName: string): boolean => options.taskFilter?.(taskName) ?? true;
  const starts = new Map<string, number>();

  const finish = (jobId: string, task: string, status: "completed" | "failed"): void => {
    const start = starts.get(jobId);
    if (start === undefined) {
      return;
    }
    starts.delete(jobId);
    metrics.activeWorkers.dec();
    metrics.jobDuration.observe({ task }, (performance.now() - start) / 1000);
    metrics.jobsTotal.inc({ task, status });
  };

  return {
    before(ctx) {
      if (!tracked(ctx.taskName)) {
        return;
      }
      metrics.activeWorkers.inc();
      starts.set(ctx.jobId, performance.now());
    },
    after(ctx) {
      finish(ctx.jobId, ctx.taskName, "completed");
    },
    onError(ctx) {
      finish(ctx.jobId, ctx.taskName, "failed");
    },
    onRetry(event) {
      if (tracked(event.taskName)) {
        metrics.retriesTotal.inc({ task: event.taskName });
      }
    },
  };
}

/** Options for {@link PrometheusStatsCollector}. */
export interface PrometheusStatsCollectorOptions extends CommonOptions {
  /** Poll interval in milliseconds (default 10000). */
  intervalMs?: number;
}

/**
 * Periodically samples queue depth and dead-letter size into gauges. Call {@link start}
 * to begin polling and {@link stop} to end it (the timer is unref'd, so it never keeps
 * the process alive on its own).
 */
export class PrometheusStatsCollector {
  private readonly metrics: FlexiQMetrics;
  private readonly intervalMs: number;
  private timer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly queue: Queue,
    options: PrometheusStatsCollectorOptions = {},
  ) {
    const namespace = options.namespace ?? "flexiq";
    const register = options.register ?? defaultRegister;
    this.metrics = getMetrics(register, namespace);
    const intervalMs = options.intervalMs ?? 10_000;
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new RangeError(`intervalMs must be a positive finite number, got ${intervalMs}`);
    }
    this.intervalMs = intervalMs;
  }

  /** Begin polling. Sampling runs immediately, then every `intervalMs`. */
  start(): void {
    if (this.timer) {
      return;
    }
    const sampleSafely = () =>
      void this.sample().catch((error) => {
        // A transient storage failure must not kill the polling loop.
        log.warn(() => "stats sample failed", error);
      });
    sampleSafely();
    this.timer = setInterval(sampleSafely, this.intervalMs);
    this.timer.unref();
  }

  /** Stop polling. */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  /** Read current queue/DLQ stats into the gauges. Public for one-shot scrapes. */
  async sample(): Promise<void> {
    for (const [name, stats] of Object.entries(await this.queue.statsAllQueues())) {
      this.metrics.queueDepth.set({ queue: name }, stats.pending);
    }
    this.metrics.dlqSize.set((await this.queue.stats()).dead);
  }
}
