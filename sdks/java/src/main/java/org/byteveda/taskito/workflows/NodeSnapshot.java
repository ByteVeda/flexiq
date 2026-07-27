package org.byteveda.taskito.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/** A node's state within a workflow run. Timestamps are Unix milliseconds; nullable when unset. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class NodeSnapshot {
    public final String nodeName;
    public final NodeStatus status;
    public final String jobId;
    public final String resultHash;
    public final Integer fanOutCount;
    public final Long startedAt;
    public final Long completedAt;
    public final String error;
    /** Saga rollback job for this node; null outside a compensation flow. */
    public final String compensationJobId;

    public final Long compensationStartedAt;
    public final Long compensationCompletedAt;
    public final String compensationError;

    @JsonCreator
    public NodeSnapshot(
            @JsonProperty("nodeName") String nodeName,
            @JsonProperty("status") NodeStatus status,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("resultHash") String resultHash,
            @JsonProperty("fanOutCount") Integer fanOutCount,
            @JsonProperty("startedAt") Long startedAt,
            @JsonProperty("completedAt") Long completedAt,
            @JsonProperty("error") String error,
            @JsonProperty("compensationJobId") String compensationJobId,
            @JsonProperty("compensationStartedAt") Long compensationStartedAt,
            @JsonProperty("compensationCompletedAt") Long compensationCompletedAt,
            @JsonProperty("compensationError") String compensationError) {
        this.nodeName = nodeName;
        this.status = status;
        this.jobId = jobId;
        this.resultHash = resultHash;
        this.fanOutCount = fanOutCount;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.error = error;
        this.compensationJobId = compensationJobId;
        this.compensationStartedAt = compensationStartedAt;
        this.compensationCompletedAt = compensationCompletedAt;
        this.compensationError = compensationError;
    }

    /**
     * How long this node ran, in milliseconds.
     *
     * @return {@code completedAt - startedAt}, or null while either timestamp is unset
     *     (the node hasn't started, or hasn't finished).
     */
    public @Nullable Long durationMs() {
        return elapsed(startedAt, completedAt);
    }

    /**
     * How long this node's compensation ran, in milliseconds.
     *
     * @return the rollback's elapsed time, or null outside a completed compensation flow.
     */
    public @Nullable Long compensationDurationMs() {
        return elapsed(compensationStartedAt, compensationCompletedAt);
    }

    private static @Nullable Long elapsed(@Nullable Long from, @Nullable Long to) {
        return from == null || to == null ? null : to - from;
    }
}
