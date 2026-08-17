package org.byteveda.flexiq.predicates;

/**
 * What this process's gates decided: one count per gated enqueue, keyed by the
 * decision that won. Enqueues of ungated tasks are not counted.
 *
 * @param allowed enqueues every gate allowed
 * @param skipped enqueues a gate skipped (no job created)
 * @param deferred enqueues a gate delayed
 * @param rejected enqueues a gate refused
 * @param errors gates that threw; the error still propagates to the enqueue caller
 */
public record PredicateStats(long allowed, long skipped, long deferred, long rejected, long errors) {}
