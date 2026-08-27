package org.byteveda.flexiq.events;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** A finished job's outcome. {@code error} is null on success/cancel; {@code retryCount} is -1 when N/A. */
public final class OutcomeEvent implements FlexiQEvent {
    /** Which outcome this is; what the emitter routes on. */
    public final EventName name;

    /** The job that reached this outcome. */
    public final String jobId;

    /** The task's registered name. */
    public final String taskName;

    /** The stored error string, or {@code null} on success and cancel. */
    public final String error;

    /** How many attempts had been spent, or {@code -1} where it does not apply. */
    public final int retryCount;

    /** Whether the attempt was cut short by the task timeout. */
    public final boolean timedOut;

    /** Execution time the worker measured; 0 when the run wasn't measured. */
    private final long wallTimeNs;

    /**
     * An outcome with no measured execution time.
     *
     * @param name which outcome this is
     * @param jobId the job that reached it
     * @param taskName the task's registered name
     * @param error the stored error string, or {@code null} on success and cancel
     * @param retryCount attempts spent, or {@code -1} where it does not apply
     * @param timedOut whether the attempt was cut short by the task timeout
     */
    public OutcomeEvent(EventName name, String jobId, String taskName, String error, int retryCount, boolean timedOut) {
        this(name, jobId, taskName, error, retryCount, timedOut, 0L);
    }

    /**
     * An outcome carrying the execution time the worker measured.
     *
     * @param name which outcome this is; must not be {@code null}, since the emitter
     *     routes on it
     * @param jobId the job that reached it
     * @param taskName the task's registered name
     * @param error the stored error string, or {@code null} on success and cancel
     * @param retryCount attempts spent, or {@code -1} where it does not apply
     * @param timedOut whether the attempt was cut short by the task timeout
     * @param wallTimeNs how long the run took, or {@code 0} when it was not measured
     */
    public OutcomeEvent(
            EventName name,
            String jobId,
            String taskName,
            String error,
            int retryCount,
            boolean timedOut,
            long wallTimeNs) {
        // Emitter keys its listener map on name() — a null here would NPE at
        // emit time, after the event escaped; fail at construction instead.
        this.name = Objects.requireNonNull(name, "name");
        this.jobId = jobId;
        this.taskName = taskName;
        this.error = error;
        this.retryCount = retryCount;
        this.timedOut = timedOut;
        this.wallTimeNs = wallTimeNs;
    }

    @Override
    public EventName name() {
        return name;
    }

    /**
     * How long the job ran, in milliseconds.
     *
     * @return the execution time, or null when nothing measured the run — a job that
     *     failed before it ever executed, or one the runtime recovered rather than a
     *     worker finishing it.
     */
    public @Nullable Long durationMs() {
        return wallTimeNs > 0 ? wallTimeNs / 1_000_000L : null;
    }
}
