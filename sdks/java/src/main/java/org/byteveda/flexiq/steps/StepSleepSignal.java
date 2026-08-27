package org.byteveda.flexiq.steps;

/**
 * Thrown by {@code step.sleep} once the sleep row is committed.
 *
 * <p>By the time this is thrown the job is already {@code Pending} at
 * {@link #wakeAt()} and this worker's claim is gone, so the body must unwind
 * now — anything it does past this point runs unclaimed and runs again on wake.
 *
 * <p>It is not a failure: the attempt ends without touching the retry count,
 * the retry budget, the circuit breaker or the task metrics.
 */
public final class StepSleepSignal extends StepControlSignal {
    private static final long serialVersionUID = 1L;

    /** Identity of the sleep, carried into the serialized form. */
    private final String stepKey;

    /** The wake deadline in Unix milliseconds, carried into the serialized form. */
    private final long wakeAt;

    /**
     * Thrown once the sleep row is committed and the deadline is still ahead.
     *
     * @param stepKey identity of the sleep the job is in
     * @param wakeAt the deadline the job was rescheduled to, in Unix milliseconds
     */
    public StepSleepSignal(String stepKey, long wakeAt) {
        super("step " + stepKey + " sleeps until " + wakeAt);
        this.stepKey = stepKey;
        this.wakeAt = wakeAt;
    }

    /**
     * Identity of the sleep the job is in.
     *
     * @return the step key the core assigned this sleep
     */
    public String stepKey() {
        return stepKey;
    }

    /**
     * The deadline the job was rescheduled to.
     *
     * @return the wake instant, in Unix milliseconds
     */
    public long wakeAt() {
        return wakeAt;
    }

    /**
     * Never: a sleep is not a failure, so there is nothing to retry. The worker
     * reports it as its own outcome rather than as an error.
     */
    @Override
    public boolean shouldRetry() {
        return false;
    }
}
