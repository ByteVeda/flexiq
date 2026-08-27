package org.byteveda.flexiq.steps;

/**
 * The swallow defence: a flag the worker checks after the task body returns.
 *
 * <p>{@link StepControlSignal} is an {@link Error}, so a
 * {@code catch (Exception e)} in a task body cannot swallow one. A
 * {@code catch (Throwable t)} can — and a body that catches a sleep and carries
 * on runs the rest of itself with no execution claim, so every side effect
 * after that point happens again on wake; a body that catches a divergence goes
 * on to return a value derived from a memo answering a different question.
 *
 * <p>So the step API latches before it throws, and the worker fails the attempt
 * if the body returns normally with the latch set.
 */
public final class StepLatch {
    private volatile boolean raised;

    /** A fresh latch for one invocation. */
    public StepLatch() {}

    /** Record that a step control signal is being thrown out of the body. */
    public void latch() {
        raised = true;
    }

    /**
     * Whether a control signal was raised at some point during this attempt.
     *
     * @return {@code true} once {@link #latch()} has been called
     */
    public boolean swallowed() {
        return raised;
    }

    /**
     * Throw if the body returned normally after swallowing a control signal.
     *
     * <p>Called the moment the handler returns, before the {@code after} hooks:
     * what the body returned is not a result, and those hooks exist to see one.
     *
     * <p>A swallowed <b>sleep</b> reaches here too, and the failure it raises is
     * then dropped — the sleep already left the job {@code Pending} and
     * unclaimed, which is exactly what the scheduler's {@code (owner, attempt)}
     * fence calls superseded. The job wakes, the sleep is a memo hit, and the
     * body finishes: one attempt wasted, nothing broken. The latch only
     * <i>bites</i> on a swallowed divergence, where the attempt still holds its
     * claim and nothing downstream would question the value it returns.
     *
     * @throws StepSwallowedError if a control signal was caught and not rethrown
     */
    public void check() {
        if (raised) {
            throw new StepSwallowedError("the task body caught a step control signal and returned anyway. "
                    + "Whatever it did after that ran without an execution claim, or on a memo answering a "
                    + "different question, so this attempt cannot be trusted. Let the step API's signals "
                    + "propagate — they are Errors precisely so an ordinary catch does not see them.");
        }
    }
}
