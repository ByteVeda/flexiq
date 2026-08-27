package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A per-execution task metric. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TaskMetric {
    /** The task that ran. */
    public final String taskName;

    /** The job this execution belonged to. */
    public final String jobId;

    /** How long the execution took, in nanoseconds. */
    public final long wallTimeNs;

    /** Peak memory the worker attributed to it, or 0 where it measured none. */
    public final long memoryBytes;

    /** Whether the handler returned rather than threw. */
    public final boolean succeeded;

    /** When the row was written, in Unix milliseconds. */
    public final long recordedAt;

    /**
     * Decoded from the core's JSON metric row.
     *
     * @param taskName the task that ran
     * @param jobId the job this execution belonged to
     * @param wallTimeNs how long the execution took, in nanoseconds
     * @param memoryBytes peak memory the worker attributed to it, or 0 where it measured none
     * @param succeeded whether the handler returned rather than threw
     * @param recordedAt when the row was written, in Unix milliseconds
     */
    @JsonCreator
    public TaskMetric(
            @JsonProperty("taskName") String taskName,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("wallTimeNs") long wallTimeNs,
            @JsonProperty("memoryBytes") long memoryBytes,
            @JsonProperty("succeeded") boolean succeeded,
            @JsonProperty("recordedAt") long recordedAt) {
        this.taskName = taskName;
        this.jobId = jobId;
        this.wallTimeNs = wallTimeNs;
        this.memoryBytes = memoryBytes;
        this.succeeded = succeeded;
        this.recordedAt = recordedAt;
    }
}
