package org.byteveda.flexiq.interception;

import org.jspecify.annotations.Nullable;

/**
 * Inspects an enqueue on the producer and decides what to do with it (see
 * {@link Interception}). Runs synchronously before serialization; keep it fast.
 */
@FunctionalInterface
public interface Interceptor {
    Interception intercept(String taskName, @Nullable Object payload);
}
