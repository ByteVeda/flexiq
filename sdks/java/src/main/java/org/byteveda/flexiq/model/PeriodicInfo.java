package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Immutable view of a registered periodic task. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PeriodicInfo {
    public final String name;
    public final String taskName;
    public final String cronExpr;
    public final String queue;
    public final boolean enabled;
    /** Last fire time, or {@code null} if it has not run yet. */
    public final Long lastRun;

    public final long nextRun;
    /** IANA timezone the cron is evaluated in, or {@code null} for UTC. */
    public final String timezone;

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
