package org.byteveda.flexiq.steps;

/**
 * This attempt lost its execution claim while a step was in flight.
 *
 * <p>The job is running under another owner right now. The attempt still
 * reports a failure — every worker path owes the scheduler a result — but the
 * scheduler fences on {@code (owner, attempt)} before it mutates anything and
 * drops this one, so the run proceeding elsewhere is untouched. Retrying would
 * only produce a second result for the same fence to drop.
 */
public final class StepSupersededError extends StepError {
    private static final long serialVersionUID = 1L;

    /**
     * A lost claim, detected when the step tried to commit.
     *
     * @param message the core's account of which owner or attempt holds the job now
     */
    public StepSupersededError(String message) {
        super(message, false);
    }
}
