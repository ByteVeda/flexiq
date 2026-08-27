package org.byteveda.flexiq.steps;

/**
 * A step result, or the job's running total, is past the cap.
 *
 * <p>The answer is not a bigger cap — it is storing the value somewhere else
 * and memoizing the handle to it. Permanent: the replay produces the same
 * bytes.
 */
public final class StepLimitExceededError extends StepError {
    private static final long serialVersionUID = 1L;

    /**
     * A cap the core enforced on a step result or on the job's running total.
     *
     * @param message the core's account of which cap was passed, and by how much
     */
    public StepLimitExceededError(String message) {
        super(message, false);
    }
}
