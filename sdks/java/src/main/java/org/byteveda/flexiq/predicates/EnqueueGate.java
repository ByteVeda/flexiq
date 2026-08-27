package org.byteveda.flexiq.predicates;

/**
 * A richer enqueue gate that maps a {@link PredicateContext} to an
 * {@link EnqueueDecision} (allow / skip / defer / reject). Evaluated
 * synchronously on the producer thread, so keep it fast and side-effect-free.
 * For a plain boolean allow/reject, use {@link Predicate} instead.
 */
@FunctionalInterface
public interface EnqueueGate {
    /**
     * Decide what to do with one enqueue.
     *
     * @param context the task, payload and options the caller passed
     * @return allow, skip, defer or reject
     */
    EnqueueDecision decide(PredicateContext context);
}
