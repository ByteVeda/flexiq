package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Immutable view of a dead-letter entry. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class DeadJob {
    public final String id;
    public final String originalJobId;
    public final String queue;
    public final String taskName;
    public final String error;
    public final int retryCount;
    public final long failedAt;
    public final String metadata;
    public final int dlqRetryCount;

    @JsonCreator
    public DeadJob(
            @JsonProperty("id") String id,
            @JsonProperty("originalJobId") String originalJobId,
            @JsonProperty("queue") String queue,
            @JsonProperty("taskName") String taskName,
            @JsonProperty("error") String error,
            @JsonProperty("retryCount") int retryCount,
            @JsonProperty("failedAt") long failedAt,
            @JsonProperty("metadata") String metadata,
            @JsonProperty("dlqRetryCount") int dlqRetryCount) {
        this.id = id;
        this.originalJobId = originalJobId;
        this.queue = queue;
        this.taskName = taskName;
        this.error = error;
        this.retryCount = retryCount;
        this.failedAt = failedAt;
        this.metadata = metadata;
        this.dlqRetryCount = dlqRetryCount;
    }
}
