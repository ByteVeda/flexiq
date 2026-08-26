package org.byteveda.flexiq.steps;

/**
 * A durable step with no result — run once for its side effect.
 *
 * <p>Nothing is memoized but the fact that it ran, which is the point: the
 * replay skips it.
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
