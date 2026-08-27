package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A task log line. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TaskLog {
    /** The log row's own id. */
    public final String id;

    /** The job that emitted it. */
    public final String jobId;

    /** That job's task name, denormalised so a task-wide query needs no join. */
    public final String taskName;

    /** The severity it was logged at. */
    public final String level;

    /** The line itself. */
    public final String message;

    /** Structured context as JSON, or {@code null}. */
    public final String extra;

    /** When it was emitted, in Unix milliseconds. */
    public final long loggedAt;

    /**
     * Decoded from the core's JSON log row.
     *
     * @param id the log row's own id
     * @param jobId the job that emitted it
     * @param taskName that job's task name, denormalised so a task-wide query needs no join
     * @param level the severity it was logged at
     * @param message the line itself
     * @param extra structured context as JSON, or {@code null}
     * @param loggedAt when it was emitted, in Unix milliseconds
     */
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
