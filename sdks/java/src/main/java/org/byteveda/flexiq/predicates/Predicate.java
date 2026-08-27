package org.byteveda.flexiq.predicates;

/**
 * A gate evaluated when a task is enqueued: returns {@code true} to allow the
 * enqueue, {@code false} to reject it. Evaluated synchronously on the producer
 * thread, so keep it fast and side-effect-free.
 */
@FunctionalInterface
public interface Predicate {
    /**
     * Decide whether one enqueue may proceed.
     *
     * @param context the task, payload and options the caller passed
     * @return {@code true} to allow the enqueue, {@code false} to reject it
     */
    boolean test(PredicateContext context);
}
