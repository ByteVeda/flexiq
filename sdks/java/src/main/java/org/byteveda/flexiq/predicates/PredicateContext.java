package org.byteveda.flexiq.predicates;

import org.jspecify.annotations.Nullable;

/**
 * What an enqueue gate sees: the task being enqueued and its payload.
 *
 * @param taskName the task's registered name
 * @param payload the payload being enqueued (before serialization)
 */
public record PredicateContext(String taskName, @Nullable Object payload) {}
