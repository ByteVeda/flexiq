package org.byteveda.flexiq.interception;

import org.jspecify.annotations.Nullable;

/**
 * Inspects an enqueue on the producer and decides what to do with it (see
 * {@link Interception}). Runs synchronously before serialization; keep it fast.
 */
@FunctionalInterface
public interface Interceptor {
    /**
     * Decide what to do with one enqueue.
     *
     * @param taskName the task the caller asked for
     * @param payload what the caller passed, before serialization
     * @return the strategy — pass, convert, redirect or reject
     */
    Interception intercept(String taskName, @Nullable Object payload);
}
