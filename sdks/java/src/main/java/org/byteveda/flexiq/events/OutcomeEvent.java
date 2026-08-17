package org.byteveda.flexiq.events;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** A finished job's outcome. {@code error} is null on success/cancel; {@code retryCount} is -1 when N/A. */
public final class OutcomeEvent implements FlexiQEvent {
    public final EventName name;
    public final String jobId;
    public final String taskName;
    public final String error;
    public final int retryCount;
    public final boolean timedOut;

    /** Execution time the worker measured; 0 when the run wasn't measured. */
    private final long wallTimeNs;

    public OutcomeEvent(EventName name, String jobId, String taskName, String error, int retryCount, boolean timedOut) {
        this(name, jobId, taskName, error, retryCount, timedOut, 0L);
    }

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
