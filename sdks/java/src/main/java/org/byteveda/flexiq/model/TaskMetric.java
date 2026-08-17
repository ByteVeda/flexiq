package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A per-execution task metric. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TaskMetric {
    public final String taskName;
    public final String jobId;
    public final long wallTimeNs;
    public final long memoryBytes;
    public final boolean succeeded;
    public final long recordedAt;

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
