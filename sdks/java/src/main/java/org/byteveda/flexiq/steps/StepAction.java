package org.byteveda.flexiq.steps;

/**
 * A durable step with no result — run for its side effect.
 *
 * <p>Nothing is memoized but the fact that it ran, which is the point: the
 * replay skips it. The same limit as {@link StepBody} applies — an action that
 * throws, or a process that dies before the row commits, runs again on the next
 * attempt, so the side effect has to be idempotent under
 * {@link StepContext#idempotencyKey()}.
 */
@FunctionalInterface
public interface StepAction {
    /**
     * Do the work.
     *
     * @throws Exception whatever the work throws; the task fails with it
     */
    void run() throws Exception;
}
