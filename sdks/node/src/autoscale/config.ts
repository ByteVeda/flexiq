/**
 * Tunables for the bare-metal autoscaler.
 *
 * Defaults mirror Kubernetes HPA semantics so the behaviour is predictable if
 * you already run KEDA or the Horizontal Pod Autoscaler:
 *
 * - `scaleDownWindowMs` of 5 minutes matches HPA's downscale stabilisation
 *   default. Scaling down on noisy queue depth is the textbook failure mode.
 * - `tolerance` of 10% matches HPA's tolerance: small fluctuations don't churn
 *   processes.
 * - `scaleUpWindowMs` of 0 absorbs bursts immediately — an extra worker for a
 *   few seconds costs far less than a backlog growing while we wait.
 */

/** Options for {@link serveAutoscaler} and {@link Autoscaler}. */
export interface AutoscaleOptions {
  /**
   * Module exporting the configured `Queue` (default export or `queue`), the
   * same shape `flexiq run <app>` loads. Each spawned worker imports it.
   */
  app: string;
  /** Never scale below this count (default 1; 0 allows idle scale-to-zero). */
  minWorkers?: number;
  /** Never scale above this count (default 10). */
  maxWorkers?: number;
  /**
   * Pending jobs per worker the depth signal aims for (default 15). Lower
   * scales up sooner; higher absorbs spikes with fewer processes.
   */
  targetQueueDepthPerWorker?: number;
  /**
   * Target `running / capacity` ratio for the utilisation signal, where
   * capacity is `workers × concurrencyPerWorker`. Must be in `(0, 1)`
   * (default 0.75).
   */
  targetUtilisation?: number;
  /**
   * Aggregation window for scale-up decisions (default 0 — immediate). Within
   * the window the *minimum* recent recommendation wins, so the pool grows
   * only once every tick in the window agrees the demand is real.
   */
  scaleUpWindowMs?: number;
  /**
   * Aggregation window for scale-down decisions (default 300000). The
   * *maximum* recent recommendation wins, so a brief lull doesn't tear the
   * pool down.
   */
  scaleDownWindowMs?: number;
  /**
   * Skip scaling when the desired count is within this fraction of the current
   * count (default 0.1). Must be in `[0, 1)`.
   */
  tolerance?: number;
  /** Milliseconds between decision ticks (default 5000). */
  pollIntervalMs?: number;
  /** SIGTERM grace period per worker before SIGKILL escalation (default 30000). */
  drainTimeoutMs?: number;
  /**
   * Jobs each spawned worker runs at once (default 4). Passed straight to the
   * child's `runWorker({ concurrency })`, so the capacity the utilisation
   * signal assumes is the capacity workers actually have.
   */
  concurrencyPerWorker?: number;
  /**
   * Queues the spawned workers consume (default: the worker default). When
   * set, the depth and utilisation signals are read from these queues only.
   */
  queues?: string[];
  /** Jobs each worker claims per scheduler poll (default: the worker default). */
  batchSize?: number;
  /** Node binary used to spawn workers (default: the autoscaler's own). */
  nodeExecutable?: string;
  /**
   * Extra flags for the Node binary (default none). Pass a loader here when
   * the app module isn't plain JavaScript, e.g. `["--import", "tsx"]`.
   */
  nodeArgs?: string[];
}

/** {@link AutoscaleOptions} with every default filled in. */
export interface AutoscaleConfig {
  readonly app: string;
  readonly minWorkers: number;
  readonly maxWorkers: number;
  readonly targetQueueDepthPerWorker: number;
  readonly targetUtilisation: number;
  readonly scaleUpWindowMs: number;
  readonly scaleDownWindowMs: number;
  readonly tolerance: number;
  readonly pollIntervalMs: number;
  readonly drainTimeoutMs: number;
  readonly concurrencyPerWorker: number;
  readonly queues: readonly string[] | undefined;
  readonly batchSize: number | undefined;
  readonly nodeExecutable: string;
  readonly nodeArgs: readonly string[];
}

/**
 * Apply defaults and validate. Throws `RangeError` on a value that would make
 * the control loop misbehave (a zero divisor, an inverted min/max) rather than
 * letting it churn processes at runtime.
 */
export function resolveAutoscaleConfig(options: AutoscaleOptions): AutoscaleConfig {
  const config: AutoscaleConfig = {
    app: options.app,
    minWorkers: options.minWorkers ?? 1,
    maxWorkers: options.maxWorkers ?? 10,
    targetQueueDepthPerWorker: options.targetQueueDepthPerWorker ?? 15,
    targetUtilisation: options.targetUtilisation ?? 0.75,
    scaleUpWindowMs: options.scaleUpWindowMs ?? 0,
    scaleDownWindowMs: options.scaleDownWindowMs ?? 300_000,
    tolerance: options.tolerance ?? 0.1,
    pollIntervalMs: options.pollIntervalMs ?? 5_000,
    drainTimeoutMs: options.drainTimeoutMs ?? 30_000,
    concurrencyPerWorker: options.concurrencyPerWorker ?? 4,
    queues: options.queues?.length ? [...options.queues] : undefined,
    batchSize: options.batchSize,
    nodeExecutable: options.nodeExecutable ?? process.execPath,
    nodeArgs: [...(options.nodeArgs ?? [])],
  };

  if (!config.app) {
    throw new RangeError("app is required — the module each worker loads");
  }
  requireInteger(config.minWorkers, "minWorkers", 0);
  requireInteger(config.maxWorkers, "maxWorkers", 1);
  if (config.maxWorkers < config.minWorkers) {
    throw new RangeError(
      `maxWorkers (${config.maxWorkers}) must be >= minWorkers (${config.minWorkers})`,
    );
  }
  requireInteger(config.targetQueueDepthPerWorker, "targetQueueDepthPerWorker", 1);
  if (!(config.targetUtilisation > 0 && config.targetUtilisation < 1)) {
    throw new RangeError(`targetUtilisation must be in (0, 1), got ${config.targetUtilisation}`);
  }
  requireInteger(config.scaleUpWindowMs, "scaleUpWindowMs", 0);
  requireInteger(config.scaleDownWindowMs, "scaleDownWindowMs", 0);
  if (!(config.tolerance >= 0 && config.tolerance < 1)) {
    throw new RangeError(`tolerance must be in [0, 1), got ${config.tolerance}`);
  }
  requireInteger(config.pollIntervalMs, "pollIntervalMs", 1);
  requireInteger(config.drainTimeoutMs, "drainTimeoutMs", 1);
  requireInteger(config.concurrencyPerWorker, "concurrencyPerWorker", 1);
  if (config.batchSize !== undefined) {
    requireInteger(config.batchSize, "batchSize", 1);
  }
  return config;
}

function requireInteger(value: number, name: string, minimum: number): void {
  if (!Number.isInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be an integer >= ${minimum}, got ${value}`);
  }
}
