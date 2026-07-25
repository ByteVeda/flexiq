/**
 * Decision loop for the bare-metal autoscaler.
 *
 * The HPA-style formula and the stabilisation windows live here; spawning and
 * draining belong to {@link WorkerProcessManager}. This module decides *how
 * many* workers we want, never how they start.
 *
 * {@link computeDesiredWorkers} is a pure function with no process or storage
 * interaction, so the decision logic is testable on its own.
 */

import type { Queue } from "../queue";
import type { Stats } from "../types";
import { createLogger } from "../utils";
import { type AutoscaleConfig, type AutoscaleOptions, resolveAutoscaleConfig } from "./config";
import { installSignalHandlers, WorkerProcessManager } from "./processManager";

const log = createLogger("autoscale");

/** The part of {@link Queue} the controller reads. */
export type AutoscaleMetricsSource = Pick<Queue, "stats" | "statsByQueue">;

/** One tick of the decision loop, returned for logging and tests. */
export interface ScaleDecision {
  /** Pending jobs observed this tick. */
  pending: number;
  /** Running jobs observed this tick. */
  running: number;
  /** Live workers before the decision was applied. */
  currentWorkers: number;
  /** Live workers the controller is moving to. */
  desiredWorkers: number;
  /** Human-readable explanation, e.g. `scale-up: depth=4 util=2 overload=false`. */
  rationale: string;
}

/** Inputs to {@link computeDesiredWorkers}. */
export interface ScaleInputs {
  pending: number;
  running: number;
  currentWorkers: number;
  config: AutoscaleConfig;
}

/**
 * Pure HPA-style decision function.
 *
 * Two signals drive the target:
 *
 * - **depth** — `ceil(pending / targetQueueDepthPerWorker)`. Mirrors the
 *   Kubernetes queue-depth scaler: each extra worker takes one target-sized
 *   slice of the backlog.
 * - **utilisation** — `ceil(workers × (utilisation / targetUtilisation))`, the
 *   canonical HPA formula. At exactly the target it returns the current count.
 *
 * The larger of the two wins, so the controller errs toward capacity. Overload
 * (`running > capacity`) forces an immediate +1 regardless of the tolerance
 * band; otherwise a desired count within `tolerance` of current is treated as
 * no change, so process counts don't churn on noise.
 */
export function computeDesiredWorkers({
  pending,
  running,
  currentWorkers,
  config,
}: ScaleInputs): ScaleDecision {
  const workers = Math.max(0, currentWorkers);
  const capacity = Math.max(1, workers * config.concurrencyPerWorker);

  const depthDesired = Math.ceil(pending / config.targetQueueDepthPerWorker);

  // Utilisation is meaningless with nothing running: leave it at 0 so the depth
  // signal and the minWorkers floor decide, letting an idle pool reach its
  // minimum instead of holding at its current size.
  const utilisation = running / capacity;
  const utilDesired =
    workers > 0 && utilisation > 0
      ? Math.ceil(workers * (utilisation / config.targetUtilisation))
      : 0;

  // A worker claimed more jobs than its concurrency can run. Tolerance would
  // otherwise sit on a genuine overload, so bypass it.
  const overloaded = workers > 0 && running > capacity;

  let desired = Math.max(depthDesired, utilDesired, config.minWorkers);
  if (overloaded) {
    desired = Math.max(desired, workers + 1);
  }
  desired = Math.min(desired, config.maxWorkers);
  desired = Math.max(desired, config.minWorkers);

  if (workers > 0 && !overloaded && Math.abs(desired - workers) / workers < config.tolerance) {
    desired = workers;
  }

  const signals = `depth=${depthDesired} util=${utilDesired}`;
  let rationale: string;
  if (desired > workers) {
    rationale = `scale-up: ${signals} overload=${overloaded}`;
  } else if (desired < workers) {
    rationale = `scale-down: ${signals}`;
  } else {
    rationale = `stable: ${signals} utilRatio=${utilisation.toFixed(2)}`;
  }

  return { pending, running, currentWorkers: workers, desiredWorkers: desired, rationale };
}

/**
 * Poll metrics, decide, scale, repeat.
 *
 * Stabilisation windows are honoured in both directions by buffering every
 * recent recommendation and taking the least aggressive one — the minimum for
 * scale-up, the maximum for scale-down. A window of 0 acts on the current
 * tick alone.
 */
export class Autoscaler {
  private readonly config: AutoscaleConfig;
  private readonly manager: WorkerProcessManager;
  /** Every recent tick's raw recommendation as `[timestamp, desired]`. */
  private history: [number, number][] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private running = false;
  /** The tick currently mid-flight, so {@link Autoscaler.stop} can wait it out. */
  private inFlightTick: Promise<ScaleDecision> | undefined;

  constructor(
    private readonly queue: AutoscaleMetricsSource,
    options: AutoscaleOptions,
  ) {
    this.config = resolveAutoscaleConfig(options);
    this.manager = new WorkerProcessManager(this.config);
  }

  /** The process manager, for inspecting or driving the pool directly. */
  get processManager(): WorkerProcessManager {
    return this.manager;
  }

  /**
   * Seed the pool at `minWorkers` and begin ticking. Returns immediately —
   * hold the instance and call {@link Autoscaler.stop} to drain.
   */
  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    for (let i = 0; i < this.config.minWorkers; i += 1) {
      this.manager.spawnWorker();
    }
    log.info(
      () =>
        `started (min=${this.config.minWorkers} max=${this.config.maxWorkers} ` +
        `targetDepth=${this.config.targetQueueDepthPerWorker} ` +
        `targetUtil=${this.config.targetUtilisation})`,
    );
    this.schedule();
  }

  /** Stop ticking and drain every worker. Safe to call more than once. */
  async stop(): Promise<void> {
    this.running = false;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    // A tick already past its metrics await would otherwise spawn into a pool
    // that shutdown() has finished draining, leaking a detached process.
    await this.inFlightTick?.catch(() => undefined);
    log.info(() => `draining ${this.manager.countLive()} workers`);
    await this.manager.shutdown();
  }

  /** Run one decision cycle. Returns the decision for logging or tests. */
  async tick(): Promise<ScaleDecision> {
    const inFlight = this.runTick();
    this.inFlightTick = inFlight;
    try {
      return await inFlight;
    } finally {
      if (this.inFlightTick === inFlight) {
        this.inFlightTick = undefined;
      }
    }
  }

  private async runTick(): Promise<ScaleDecision> {
    const { pending, running } = await this.gatherMetrics();
    // Replace crashed workers first so the decision sees the real pool size.
    const crashed = this.manager.reapDead().length;
    for (let i = 0; i < crashed && this.manager.countLive() < this.config.minWorkers; i += 1) {
      this.manager.spawnWorker();
    }
    const current = this.manager.countLive();
    const decision = computeDesiredWorkers({
      pending,
      running,
      currentWorkers: current,
      config: this.config,
    });
    const smoothed = this.applyWindows(current, decision.desiredWorkers);
    this.applyDecision(current, smoothed);
    if (smoothed === decision.desiredWorkers) {
      return decision;
    }
    return {
      ...decision,
      desiredWorkers: smoothed,
      rationale: `${decision.rationale} (windowed -> ${smoothed})`,
    };
  }

  /** Schedule the next tick; a slow tick delays the next one rather than overlapping. */
  private schedule(): void {
    this.timer = setTimeout(() => {
      void this.tick()
        .then((decision) => {
          log.info(
            () =>
              `pending=${decision.pending} running=${decision.running} ` +
              `workers=${decision.currentWorkers} -> ${decision.desiredWorkers} ` +
              `(${decision.rationale})`,
          );
        })
        .catch((error: unknown) => {
          log.error(() => "tick failed", error);
        })
        .finally(() => {
          if (this.running) {
            this.schedule();
          }
        });
    }, this.config.pollIntervalMs);
  }

  /**
   * Read pending / running for the queues the workers consume. A failed read
   * reports no load rather than throwing; the scale-down window is what keeps
   * one bad sample from draining a pool that's legitimately above
   * `minWorkers`.
   */
  private async gatherMetrics(): Promise<{ pending: number; running: number }> {
    try {
      const queues = this.config.queues;
      if (!queues) {
        return summarise([await this.queue.stats()]);
      }
      return summarise(await Promise.all(queues.map((name) => this.queue.statsByQueue(name))));
    } catch (error) {
      log.error(() => "failed to gather queue stats", error);
      return { pending: 0, running: 0 };
    }
  }

  /**
   * Smooth `desired` through the scale-up / scale-down windows: the least
   * aggressive recommendation in the window wins — the minimum when growing,
   * the maximum when shrinking.
   *
   * Every tick is recorded, including the ones that asked for no change.
   * Recording only direction changes would mean the first dip after a stable
   * stretch had nothing to be compared against, and would take effect
   * immediately — exactly the flap the window exists to absorb.
   */
  private applyWindows(current: number, desired: number): number {
    const now = performance.now();
    const longestWindow = Math.max(this.config.scaleUpWindowMs, this.config.scaleDownWindowMs);
    this.history.push([now, desired]);
    this.history = this.history.filter(([at]) => at >= now - longestWindow);

    // The entry just pushed always survives its own cutoff, so neither
    // reduction below can see an empty window.
    if (desired > current) {
      return reduceWindow(this.history, now - this.config.scaleUpWindowMs, Math.min);
    }
    if (desired < current) {
      return reduceWindow(this.history, now - this.config.scaleDownWindowMs, Math.max);
    }
    return current;
  }

  private applyDecision(current: number, desired: number): void {
    if (desired > current) {
      for (let i = current; i < desired; i += 1) {
        this.manager.spawnWorker();
      }
      return;
    }
    // Drain the oldest workers first, in the background: a worker finishes its
    // in-flight jobs before leaving, and the loop must not stall for it.
    for (const pid of this.manager.livePids().slice(0, current - desired)) {
      void this.manager.terminateWorker(pid).catch((error: unknown) => {
        log.error(() => `failed to drain worker pid=${pid}`, error);
      });
    }
  }
}

/**
 * Run the autoscaler until SIGTERM or SIGINT, then drain the pool.
 *
 * ```ts
 * import { Queue, serveAutoscaler } from "@byteveda/taskito";
 *
 * const queue = new Queue({ dbPath: "taskito.db" });
 * await serveAutoscaler(queue, { app: "./app.js", minWorkers: 2, maxWorkers: 20 });
 * ```
 */
export async function serveAutoscaler(
  queue: AutoscaleMetricsSource,
  options: AutoscaleOptions,
): Promise<void> {
  const autoscaler = new Autoscaler(queue, options);
  let removeHandlers = (): void => {};
  const signalled = new Promise<void>((done) => {
    removeHandlers = installSignalHandlers(done);
  });
  try {
    autoscaler.start();
    await signalled;
  } finally {
    removeHandlers();
    await autoscaler.stop();
  }
}

/** Fold the recommendations at or after `cutoff` with `pick` (`Math.min`/`Math.max`). */
function reduceWindow(
  history: readonly [number, number][],
  cutoff: number,
  pick: (...values: number[]) => number,
): number {
  return pick(...history.filter(([at]) => at >= cutoff).map(([, value]) => value));
}

function summarise(all: Stats[]): { pending: number; running: number } {
  return {
    pending: all.reduce((total, stats) => total + stats.pending, 0),
    running: all.reduce((total, stats) => total + stats.running, 0),
  };
}
