package org.byteveda.flexiq.events;

/**
 * An attempt that ended in a durable {@code step.sleep} ({@code job.sleeping}).
 *
 * <p>Not an outcome: the job is {@code Pending} at {@link #wakeAt} and will run
 * again from its memoized steps. Nothing about the job's retry count, retry
 * budget, circuit breaker or task metrics moved.
 *
 * <p>Emitted by the worker on the thread the task slept on, alongside
 * {@link org.byteveda.flexiq.middleware.Middleware#onSleep}.
 */
public final class SleepEvent implements FlexiQEvent {
    public final String jobId;
    public final String taskName;

    /** Identity of the sleep step, so a subscriber can tell one sleep from another. */
    public final String stepKey;

    /** The deadline the job was rescheduled to, in Unix milliseconds. */
    public final long wakeAt;

    /** How long this attempt ran before it slept, in milliseconds. */
    public final long durationMs;

    public SleepEvent(String jobId, String taskName, String stepKey, long wakeAt, long durationMs) {
        this.jobId = jobId;
        this.taskName = taskName;
        this.stepKey = stepKey;
        this.wakeAt = wakeAt;
        this.durationMs = durationMs;
    }

    @Override
    public EventName name() {
        return EventName.JOB_SLEEPING;
    }
}
