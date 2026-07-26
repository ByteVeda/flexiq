/**
 * Producer-side enqueue interception. Interceptors registered via
 * `Queue.intercept` run at the very start of every enqueue — before per-task
 * defaults, middleware, gates, and serialization — and decide what happens
 * to the call:
 *
 * - `pass` — enqueue unchanged.
 * - `convert` — replace the args (task name unchanged), e.g. redact a field.
 * - `redirect` — enqueue a different task with new args instead.
 * - `reject` — block the enqueue; `Queue.enqueue` throws `InterceptionError`.
 *
 * Interceptors chain in registration order, each seeing the previous one's
 * output. Distinct from `Middleware.onEnqueue` (mutate-in-place hooks) and
 * from the Python SDK's argument-type interception.
 */
export type Interception =
  | { readonly type: "pass" }
  | { readonly type: "convert"; readonly args: unknown[] }
  | { readonly type: "redirect"; readonly taskName: string; readonly args: unknown[] }
  | { readonly type: "reject"; readonly reason: string };

/** Decides the outcome of an enqueue. Runs synchronously before serialization. */
export type Interceptor = (taskName: string, args: unknown[]) => Interception;

/** Factories for the four {@link Interception} outcomes. */
export const Interception = {
  /** Enqueue the original args unchanged. */
  pass(): Interception {
    return { type: "pass" };
  },
  /** Replace the args; the task name stays the same. */
  convert(args: unknown[]): Interception {
    return { type: "convert", args };
  },
  /** Enqueue a different task with new args instead of the original. */
  redirect(taskName: string, args: unknown[]): Interception {
    return { type: "redirect", taskName, args };
  },
  /** Block the enqueue; the reason is surfaced on the thrown error. */
  reject(reason: string): Interception {
    return { type: "reject", reason };
  },
};

/**
 * What the registered interceptors would do with one enqueue, without
 * enqueuing anything. Returned by `Queue.analyzeArguments`.
 */
export interface InterceptionAnalysis {
  /** Task that would be enqueued — differs from the input after a `redirect`. */
  taskName: string;
  /** Args that would be serialized, after every `convert`/`redirect`. */
  args: unknown[];
  /** One entry per interceptor that ran, in registration order. */
  outcomes: Interception[];
  /**
   * True when a real enqueue would throw `InterceptionError`. `taskName` and
   * `args` then hold the original input — the chain stopped part-way, and
   * `outcomes` shows how far it got.
   */
  rejected: boolean;
  /** Message the real enqueue would throw; absent unless `rejected`. */
  rejectionReason?: string;
}

/** Dashboard wire shape for enqueue-interception stats. */
export interface InterceptionStatsSnapshot {
  total_intercepts: number;
  total_duration_ms: number;
  avg_duration_ms: number;
  strategy_counts: Record<string, number>;
  max_depth_reached: number;
}

/**
 * In-process accumulator over interceptor runs. `max_depth_reached` counts
 * the deepest interceptor chain applied to one enqueue (this SDK intercepts
 * whole enqueues, not nested argument trees).
 */
export class InterceptionMetrics {
  private totalIntercepts = 0;
  private totalDurationMs = 0;
  private maxDepth = 0;
  private readonly strategyCounts = new Map<string, number>();

  record(outcomes: readonly Interception[], durationMs: number): void {
    this.totalIntercepts += 1;
    this.totalDurationMs += durationMs;
    this.maxDepth = Math.max(this.maxDepth, outcomes.length);
    for (const outcome of outcomes) {
      this.strategyCounts.set(outcome.type, (this.strategyCounts.get(outcome.type) ?? 0) + 1);
    }
  }

  toDict(): InterceptionStatsSnapshot {
    const round2 = (value: number) => Math.round(value * 100) / 100;
    return {
      total_intercepts: this.totalIntercepts,
      total_duration_ms: round2(this.totalDurationMs),
      avg_duration_ms: round2(
        this.totalIntercepts > 0 ? this.totalDurationMs / this.totalIntercepts : 0,
      ),
      strategy_counts: Object.fromEntries(this.strategyCounts),
      max_depth_reached: this.maxDepth,
    };
  }
}
