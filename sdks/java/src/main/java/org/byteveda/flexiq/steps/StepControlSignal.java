package org.byteveda.flexiq.steps;

/**
 * Base for everything the step API throws to end an attempt.
 *
 * <p><b>Deliberately a {@link Error}, not an {@link Exception}.</b> A step body
 * is ordinary code that catches its own failures, and a
 * {@code catch (Exception e)} around a payment call must not also swallow "the
 * recorded step sequence and this code no longer agree" or "this attempt is
 * over, the job sleeps until 09:00". Java has the tier for exactly this, and
 * the Python shell spells the same idea {@code BaseException}.
 *
 * <p>{@code catch (Throwable t)} still sees one, so there is a second layer:
 * the worker latches whenever one of these is thrown and fails the attempt if
 * the body returns normally anyway (see {@link StepSwallowedError}). Catching
 * one therefore buys nothing and costs a clear error message — let it
 * propagate.
 */
public abstract class StepControlSignal extends Error {
    private static final long serialVersionUID = 1L;

    /** @param message what happened, in the core's own words where it came from the core */
    protected StepControlSignal(String message) {
        super(message);
    }

    /**
     * Whether the attempt this ended may be retried.
     *
     * <p>Decided by the core's step-failure classification, not by the task's
     * {@code retryOn} predicate: that predicate has an opinion about the task's
     * own exceptions and nothing useful to say about a divergence. The worker
     * reads this first.
     *
     * @return {@code true} to spend a retry, {@code false} to dead-letter now
     */
    public abstract boolean shouldRetry();
}
