import type { Worker } from "@/lib/api-types";

/** A worker is "stale" once its last heartbeat is older than this. */
export const WORKER_STALE_AFTER_MS = 30_000;

/**
 * Whether a worker has missed its heartbeat window. Computed against a
 * caller-supplied `now` so callers that re-render on a clock tick keep the
 * status fresh (the heartbeat ages relative to wall-clock, not a frozen ref).
 */
export function isWorkerStale(worker: Worker, now: number = Date.now()): boolean {
  return now - worker.last_heartbeat > WORKER_STALE_AFTER_MS;
}

/**
 * Registry fingerprints that are not the fleet's agreed-on one.
 *
 * The column exists to spot the odd worker out, so the largest group of workers
 * running the same task registry is taken as the intended one and every other
 * fingerprint is flagged. Workers that report no fingerprint take no part: an
 * SDK that predates the field, or a worker with nothing registered, is a
 * registry nobody can see rather than one that differs.
 *
 * A tie for largest flags every group. With no majority there is no intended
 * registry to measure against, and picking one arbitrarily would clear half a
 * split fleet of a problem it has.
 */
export function divergentFingerprints(workers: readonly Worker[]): ReadonlySet<string> {
  const counts = new Map<string, number>();
  for (const worker of workers) {
    const fingerprint = worker.registry_fingerprint;
    if (fingerprint) {
      counts.set(fingerprint, (counts.get(fingerprint) ?? 0) + 1);
    }
  }
  if (counts.size < 2) {
    return new Set();
  }
  const largest = Math.max(...counts.values());
  const leaders = [...counts.keys()].filter((fingerprint) => counts.get(fingerprint) === largest);
  if (leaders.length !== 1) {
    return new Set(counts.keys());
  }
  const [agreed] = leaders;
  return new Set([...counts.keys()].filter((fingerprint) => fingerprint !== agreed));
}
