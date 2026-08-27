package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Immutable view of a dead-letter entry. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class DeadJob {
    /** The dead-letter row's own id. */
    public final String id;

    /** The job that failed, which no longer exists in the queue. */
    public final String originalJobId;

    /** The queue it had been enqueued to. */
    public final String queue;

    /** The task's registered name. */
    public final String taskName;

    /** The final attempt's stored error; decode with {@code TaskErrors}. */
    public final String error;

    /** How many attempts were spent before it was given up on. */
    public final int retryCount;

    /** When it was dead-lettered, in Unix milliseconds. */
    public final long failedAt;

    /** The opaque metadata blob attached at enqueue, or {@code null}. */
    public final String metadata;

    /** How many times an operator has re-enqueued it from here. */
    public final int dlqRetryCount;

    /**
     * Decoded from the core's JSON dead-letter view.
     *
     * @param id the dead-letter row's own id
     * @param originalJobId the job that failed, which no longer exists in the queue
     * @param queue the queue it had been enqueued to
     * @param taskName the task's registered name
     * @param error the final attempt's stored error; decode with {@code TaskErrors}
     * @param retryCount how many attempts were spent before it was given up on
     * @param failedAt when it was dead-lettered, in Unix milliseconds
     * @param metadata the opaque metadata blob attached at enqueue, or {@code null}
     * @param dlqRetryCount how many times an operator has re-enqueued it from here
     */
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
