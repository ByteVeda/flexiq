package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Immutable view of a registered periodic task. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PeriodicInfo {
    /** The schedule's own identity. */
    public final String name;

    /** The task enqueued on each firing. */
    public final String taskName;

    /** The cron expression deciding when it fires. */
    public final String cronExpr;

    /** The queue the jobs go to. */
    public final String queue;

    /** Whether the schedule fires; a disabled one keeps its registration. */
    public final boolean enabled;
    /** Last fire time, or {@code null} if it has not run yet. */
    public final Long lastRun;

    /** When it fires next, in Unix milliseconds. */
    public final long nextRun;
    /** IANA timezone the cron is evaluated in, or {@code null} for UTC. */
    public final String timezone;

    /**
     * Decoded from the core's JSON schedule row.
     *
     * @param name the schedule's own identity
     * @param taskName the task enqueued on each firing
     * @param cronExpr the cron expression deciding when it fires
     * @param queue the queue the jobs go to
     * @param enabled whether the schedule fires; a disabled one keeps its registration
     * @param lastRun last fire time, or {@code null} if it has not run yet
     * @param nextRun when it fires next, in Unix milliseconds
     * @param timezone IANA timezone the cron is evaluated in, or {@code null} for UTC
     */
    @JsonCreator
    public PeriodicInfo(
            @JsonProperty("name") String name,
            @JsonProperty("taskName") String taskName,
            @JsonProperty("cronExpr") String cronExpr,
            @JsonProperty("queue") String queue,
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("lastRun") Long lastRun,
            @JsonProperty("nextRun") long nextRun,
            @JsonProperty("timezone") String timezone) {
        this.name = name;
        this.taskName = taskName;
        this.cronExpr = cronExpr;
        this.queue = queue;
        this.enabled = enabled;
        this.lastRun = lastRun;
        this.nextRun = nextRun;
        this.timezone = timezone;
    }
}
