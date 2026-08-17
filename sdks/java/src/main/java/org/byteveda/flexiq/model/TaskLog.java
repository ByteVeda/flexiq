package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A task log line. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TaskLog {
    public final String id;
    public final String jobId;
    public final String taskName;
    public final String level;
    public final String message;
    public final String extra;
    public final long loggedAt;

    @JsonCreator
    public TaskLog(
            @JsonProperty("id") String id,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("taskName") String taskName,
            @JsonProperty("level") String level,
            @JsonProperty("message") String message,
            @JsonProperty("extra") String extra,
            @JsonProperty("loggedAt") long loggedAt) {
        this.id = id;
        this.jobId = jobId;
        this.taskName = taskName;
        this.level = level;
        this.message = message;
        this.extra = extra;
        this.loggedAt = loggedAt;
    }
}
