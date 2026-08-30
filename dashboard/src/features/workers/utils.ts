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

/** The queue names a worker consumes, parsed from the CSV its row carries. */
export function parseQueues(queues: string): string[] {
  return queues
    .split(",")
    .map((queue) => queue.trim())
    .filter(Boolean);
}

/**
 * Workers partitioned so that any two whose queue sets intersect land in the
 * same group, transitively.
 *
 * Comparing registries across the whole page flags a heterogeneous fleet for a
 * divergence it does not have — an `email` worker and a `video` worker never
 * see each other's jobs. Comparing only exact queue sets hides the opposite
 * case: a worker on `default,email` and a worker on `default` are not the same
 * group, yet a `default` job lands on either. Transitive overlap keeps that
 * pair together and still leaves disjoint fleets apart.
 */
function queueComponents(workers: readonly Worker[]): Worker[][] {
  const parent = workers.map((_, index) => index);

  const find = (index: number): number => {
    let root = index;
    while (parent[root] !== root) {
      root = parent[root] as number;
    }
    // Path compression: the same worker is looked up once per queue it serves.
    let walk = index;
    while (parent[walk] !== root) {
      const next = parent[walk] as number;
      parent[walk] = root;
      walk = next;
    }
    return root;
  };

  const firstOnQueue = new Map<string, number>();
  workers.forEach((worker, index) => {
    for (const queue of parseQueues(worker.queues)) {
      const other = firstOnQueue.get(queue);
      if (other === undefined) {
        firstOnQueue.set(queue, index);
      } else {
        parent[find(index)] = find(other);
      }
    }
  });

  const grouped = new Map<number, Worker[]>();
  workers.forEach((worker, index) => {
    const root = find(index);
    const members = grouped.get(root);
    if (members) {
      members.push(worker);
    } else {
      grouped.set(root, [worker]);
    }
  });
  return [...grouped.values()];
}

/**
 * Registry fingerprints within one group that are not the group's agreed-on one.
 *
 * The largest group of workers running the same task registry is taken as the
 * intended one and every other fingerprint is flagged. Workers that report no
 * fingerprint take no part: an SDK that predates the field, or a worker with
 * nothing registered, is a registry nobody can see rather than one that differs.
 *
 * A tie for largest flags every fingerprint. With no majority there is no
 * intended registry to measure against, and picking one arbitrarily would clear
 * half a split fleet of a problem it has.
 */
function oddFingerprints(workers: readonly Worker[]): ReadonlySet<string> {
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

/**
 * Ids of the workers whose task registry is not the agreed-on one of the group
 * they reach through shared queues.
 *
 * The verdict is per worker rather than per fingerprint because it is only
 * meaningful inside a queue group: one registry can be the odd one out among
 * the `default` workers and the agreed-on one among the `video` workers, and
 * flagging the fingerprint would badge both.
 *
 * The comparison spans the whole component, not just a worker's immediate
 * queue-mates, so a flagged worker's own queue can agree with it — it is linked
 * to the majority through a worker that serves both. That is deliberate: the
 * component is one interlinked set of queues, and a fleet serving it off two
 * builds is worth surfacing wherever along the chain the mismatch sits.
 *
 * Always a diagnostic, never a gate — nothing refuses work over it.
 */
export function divergentWorkers(workers: readonly Worker[]): ReadonlySet<string> {
  const flagged = new Set<string>();
  for (const component of queueComponents(workers)) {
    const odd = oddFingerprints(component);
    if (odd.size === 0) {
      continue;
    }
    for (const worker of component) {
      if (worker.registry_fingerprint && odd.has(worker.registry_fingerprint)) {
        flagged.add(worker.worker_id);
      }
    }
  }
  return flagged;
}
