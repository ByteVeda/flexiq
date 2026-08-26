package org.byteveda.flexiq.steps;

/**
 * The body of a durable step: run on the attempt that commits it, its result
 * replayed to every later attempt of the job.
 *
 * <p>"Runs once" is the guarantee for a body that <b>returns</b>. A body that
 * throws commits nothing, so the next attempt runs it again — and so does an
 * attempt whose process dies between the body returning and the row committing.
 * Memoization closes the replay window, not the crash window; only a key the
 * downstream service dedupes on closes that, which is what
 * {@link StepContext#idempotencyKey()} is for. Write side effects to be
 * idempotent under that key.
 *
 * <p>Declares {@code throws Exception} on purpose. A step body is ordinary code
 * — a network call, a database write — and forcing it to be exception-free
 * would push every caller into a {@code try/catch} that has to decide what to
 * do with a failure it cannot classify. A task handler already throws
 * {@code Exception}, so a body's failure propagates as the task's failure and
 * the task's own {@code retryOn} predicate gets its say about it.
 *
 * @param <T> the step's result type
 */
@FunctionalInterface
public interface StepBody<T> {
    /**
     * Do the work.
     *
     * @return the step's result, which is serialized and committed
     * @throws Exception whatever the work throws; the task fails with it
     */
    T get() throws Exception;
}
