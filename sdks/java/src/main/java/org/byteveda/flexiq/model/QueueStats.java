package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Job counts by status across all queues. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class QueueStats {
    public final long pending;
    public final long running;
    public final long completed;
    public final long failed;
    public final long dead;
    public final long cancelled;

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
