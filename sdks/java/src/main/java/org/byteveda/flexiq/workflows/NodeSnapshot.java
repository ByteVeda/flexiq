package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/** A node's state within a workflow run. Timestamps are Unix milliseconds; nullable when unset. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class NodeSnapshot {
    /** The node's name within its workflow. */
    public final String nodeName;

    /** Where the node stands right now. */
    public final NodeStatus status;

    /** The job this node dispatched, or {@code null} for a control node or one that never ran. */
    public final String jobId;

    /** Digest of the node's result, used to detect a cache hit. */
    public final String resultHash;

    /** How many children a fan-out node produced, or {@code null} when it is not one. */
    public final Integer fanOutCount;

    /** When the node's job was claimed, in Unix milliseconds, or {@code null}. */
    public final Long startedAt;

    /** When the node settled, in Unix milliseconds, or {@code null}. */
    public final Long completedAt;

    /** Why the node failed, or {@code null}. */
    public final String error;
    /** Saga rollback job for this node; null outside a compensation flow. */
    public final String compensationJobId;

    /** When the rollback job was claimed, in Unix milliseconds, or {@code null}. */
    public final Long compensationStartedAt;

    /** When the rollback settled, in Unix milliseconds, or {@code null}. */
    public final Long compensationCompletedAt;

    /** Why the rollback failed, or {@code null}. */
    public final String compensationError;

    /**
     * Decoded from the core's JSON node view.
     *
     * @param nodeName the node's name within its workflow
     * @param status where the node stands right now
     * @param jobId the job this node dispatched, or {@code null} for a control node or one that never ran
     * @param resultHash digest of the node's result, used to detect a cache hit
     * @param fanOutCount how many children a fan-out node produced, or {@code null} when it is not one
     * @param startedAt when the node's job was claimed, in Unix milliseconds, or {@code null}
     * @param completedAt when the node settled, in Unix milliseconds, or {@code null}
     * @param error why the node failed, or {@code null}
     * @param compensationJobId saga rollback job for this node; null outside a compensation flow
     * @param compensationStartedAt when the rollback job was claimed, in Unix milliseconds, or {@code null}
     * @param compensationCompletedAt when the rollback settled, in Unix milliseconds, or {@code null}
     * @param compensationError why the rollback failed, or {@code null}
     */
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
