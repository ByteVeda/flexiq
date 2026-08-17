package org.byteveda.flexiq.predicates;

/**
 * A gate evaluated when a task is enqueued: returns {@code true} to allow the
 * enqueue, {@code false} to reject it. Evaluated synchronously on the producer
 * thread, so keep it fast and side-effect-free.
 */
@FunctionalInterface
public interface Predicate {
    boolean test(PredicateContext context);
}
