package org.byteveda.flexiq.task;

/** A task handler: receives a deserialized payload, returns a result (or null). */
@FunctionalInterface
public interface TaskFunction<T, R> {
    R apply(T payload) throws Exception;
}
