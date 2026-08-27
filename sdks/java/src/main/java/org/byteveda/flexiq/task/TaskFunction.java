package org.byteveda.flexiq.task;

/**
 * A task handler: receives a deserialized payload, returns a result (or null).
 *
 * @param <T> the payload type the handler is called with
 * @param <R> the result type it returns
 */
@FunctionalInterface
public interface TaskFunction<T, R> {
    /**
     * Run the task.
     *
     * @param payload the deserialized argument
     * @return the result to store, or {@code null} for a handler with nothing to return
     * @throws Exception to fail the attempt; the core decides whether it retries
     */
    R apply(T payload) throws Exception;
}
