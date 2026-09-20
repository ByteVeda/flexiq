// The completion sink, in JavaScript.
//
// A hand-kept copy of `bench/harness/sink.py`: the same list name, the same
// three-field record, the same one write per job. It is duplicated rather than
// shared because the alternative is a package boundary across two ecosystems
// for four lines of code — but it does have to stay in step, and the schema
// check in `harness/report.py` is what notices when it has not.

import { Redis } from "ioredis";

export const PREFIX = "flexiq-bench";

export const sinkKey = (system) => `${PREFIX}:sink:${system}`;

/** Nanoseconds on the same clock the Python producers read.
 *
 *  `Date.now()` is milliseconds, which is coarser than the percentiles this
 *  harness reports, so the sub-millisecond part comes from `performance.now()`
 *  against its own time origin. Both sides end up reading the same wall clock,
 *  which is what makes a Python enqueue and a Node completion subtractable.
 */
export function nowNs() {
  return BigInt(Math.round((performance.timeOrigin + performance.now()) * 1e6));
}

export function connect(url) {
  return new Redis(url, { maxRetriesPerRequest: null });
}

export async function record(client, system, seq, enqueuedNs) {
  await client.rpush(
    sinkKey(system),
    JSON.stringify([seq, Number(enqueuedNs), Number(nowNs())]),
  );
}
