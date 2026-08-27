package org.byteveda.flexiq.task;

/**
 * A task handler: receives a deserialized payload, returns a result (or null).
 *
 * @param <T> the payload type the handler is called with
 * @param <R> the result type it returns
 */
@FunctionalInterface
public interface TaskFunction<T, R> {
    R apply(T payload) throws Exception;
}
