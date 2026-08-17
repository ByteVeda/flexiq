package org.byteveda.flexiq.predicates;

import java.util.concurrent.atomic.LongAdder;

/**
 * In-process counters over gate outcomes, behind {@code FlexiQ.predicateStats()}.
 * One increment per gated enqueue — the decision that won, not per gate evaluated.
 * Counted with {@link LongAdder} because enqueues run on many threads and the
 * counters are read far less often than they are bumped.
 */
public final class PredicateMetrics {
    private final LongAdder allowed = new LongAdder();
    private final LongAdder skipped = new LongAdder();
    private final LongAdder deferred = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder errors = new LongAdder();

    /** Record the decision that won for one gated enqueue. */
    public void record(EnqueueDecision decision) {
        if (decision instanceof EnqueueDecision.Skip) {
            skipped.increment();
        } else if (decision instanceof EnqueueDecision.Defer) {
            deferred.increment();
        } else if (decision instanceof EnqueueDecision.Reject) {
            rejected.increment();
        } else {
            allowed.increment();
        }
    }

    /** Record a gate that threw. */
    public void recordError() {
        errors.increment();
    }

    /** A point-in-time snapshot of the counters. */
    public PredicateStats snapshot() {
        return new PredicateStats(allowed.sum(), skipped.sum(), deferred.sum(), rejected.sum(), errors.sum());
    }
}
