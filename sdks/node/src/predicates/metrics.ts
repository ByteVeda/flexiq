import type { DecisionKind } from "./decisions";

/**
 * What the gates decided, as returned by `Queue.predicateStats`. Node's keys
 * follow its decision kinds; the Python SDK's equivalents are `denied` for
 * `rejected` and `cancelled` for `skipped`.
 */
export interface PredicateStats {
  allowed: number;
  skipped: number;
  deferred: number;
  rejected: number;
  /** Gates that threw. The error still propagates to the enqueue caller. */
  errors: number;
}

const COUNTER_FOR = {
  allow: "allowed",
  skip: "skipped",
  defer: "deferred",
  reject: "rejected",
} as const satisfies Record<DecisionKind, keyof PredicateStats>;

/**
 * In-process counters over gate outcomes — one increment per gated enqueue (the
 * decision that won), not per gate evaluated. No locking: JavaScript runs the
 * whole evaluation on one thread.
 */
export class PredicateMetrics {
  private readonly counts: PredicateStats = {
    allowed: 0,
    skipped: 0,
    deferred: 0,
    rejected: 0,
    errors: 0,
  };

  record(kind: DecisionKind): void {
    this.counts[COUNTER_FOR[kind]] += 1;
  }

  recordError(): void {
    this.counts.errors += 1;
  }

  snapshot(): PredicateStats {
    return { ...this.counts };
  }

  reset(): void {
    for (const key of Object.keys(this.counts) as (keyof PredicateStats)[]) {
      this.counts[key] = 0;
    }
  }
}
