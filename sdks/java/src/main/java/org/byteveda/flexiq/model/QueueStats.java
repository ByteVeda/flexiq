package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Job counts by status across all queues. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class QueueStats {
    /** Jobs waiting to run. */
    public final long pending;

    /** Jobs a worker currently holds a claim on. */
    public final long running;

    /** Jobs that finished successfully. */
    public final long completed;

    /** Jobs whose last attempt failed and that will be tried again. */
    public final long failed;

    /** Jobs that exhausted their retries. */
    public final long dead;

    /** Jobs cancelled before they reached a verdict. */
    public final long cancelled;

    /**
     * Decoded from the core's JSON stats view.
     *
     * @param pending jobs waiting to run
     * @param running jobs a worker currently holds a claim on
     * @param completed jobs that finished successfully
     * @param failed jobs whose last attempt failed and that will be tried again
     * @param dead jobs that exhausted their retries
     * @param cancelled jobs cancelled before they reached a verdict
     */
    @JsonCreator
    public QueueStats(
            @JsonProperty("pending") long pending,
            @JsonProperty("running") long running,
            @JsonProperty("completed") long completed,
            @JsonProperty("failed") long failed,
            @JsonProperty("dead") long dead,
            @JsonProperty("cancelled") long cancelled) {
        this.pending = pending;
        this.running = running;
        this.completed = completed;
        this.failed = failed;
        this.dead = dead;
        this.cancelled = cancelled;
    }
}
