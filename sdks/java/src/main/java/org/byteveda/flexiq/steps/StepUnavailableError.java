package org.byteveda.flexiq.steps;

/**
 * Durable steps are not available where this task is running.
 *
 * <p>Raised by an attached executor (which holds no storage and no channel to
 * commit on), by a backend with no step store, and by a control that cannot
 * fence a step on an execution claim.
 *
 * <p>The attempt fails rather than running the step un-memoized: there is no
 * version of "your charge step silently lost its memo" that beats a failure
 * naming the reason. Retryable, because a heterogeneous fleet mid-rollout may
 * put the next attempt on a worker that can commit.
 */
public final class StepUnavailableError extends StepError {
    private static final long serialVersionUID = 1L;

    /** @param message why this process cannot commit a step */
    public StepUnavailableError(String message) {
        super(message, true);
    }
}
