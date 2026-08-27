package org.byteveda.flexiq.steps;

/**
 * A step operation failed.
 *
 * <p>{@link #shouldRetry()} comes from the core: a divergence, a size cap or a
 * value that will not encode will be just as wrong next attempt, while an
 * unreachable backend may not be. The subclasses are what the native layer
 * throws, so the class alone carries the verdict across JNI.
 */
public class StepError extends StepControlSignal {
    private static final long serialVersionUID = 1L;

    /** The core's verdict, carried across JNI and into the serialized form. */
    private final boolean shouldRetry;

    /**
     * A permanent step failure.
     *
     * @param message what the step was doing and why it could not be done
     */
    public StepError(String message) {
        this(message, false);
    }

    /**
     * A step failure that retries only if {@code shouldRetry}.
     *
     * @param message what the step was doing and why it could not be done
     * @param shouldRetry {@code true} where the next attempt could plausibly get
     *     further — an unreachable backend, not a divergence
     */
    public StepError(String message, boolean shouldRetry) {
        super(message);
        this.shouldRetry = shouldRetry;
    }

    @Override
    public final boolean shouldRetry() {
        return shouldRetry;
    }
}
