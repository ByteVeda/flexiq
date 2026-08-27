package org.byteveda.flexiq.steps;

/**
 * The recorded step sequence and the running code no longer agree.
 *
 * <p>Loud and permanent by design. Handing a memoized result to a step that now
 * asks a different question is worse than re-running the step, and the next
 * attempt would replay into the same disagreement, so the retry budget must not
 * be spent reaching the same dead letter.
 *
 * <p>Thrown by the native layer; the class is how the core's verdict crosses.
 */
public final class StepDivergedError extends StepError {
    private static final long serialVersionUID = 1L;

    /**
     * A divergence the core detected while replaying the step sequence.
     *
     * @param message the core's account of which step diverged and how
     */
    public StepDivergedError(String message) {
        super(message, false);
    }
}
