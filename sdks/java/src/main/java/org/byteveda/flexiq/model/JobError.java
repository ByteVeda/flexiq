package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One recorded error attempt for a job. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobError {
    /** The history row's own id. */
    public final String id;

    /** The job this attempt belonged to. */
    public final String jobId;

    /** Which attempt it was, counted from zero. */
    public final int attempt;

    /** The stored error string; decode with {@code TaskErrors}. */
    public final String error;

    /** When the attempt failed, in Unix milliseconds. */
    public final long failedAt;

    /**
     * Decoded from the core's JSON error-history view.
     *
     * @param id the history row's own id
     * @param jobId the job this attempt belonged to
     * @param attempt which attempt it was, counted from zero
     * @param error the stored error string; decode with {@code TaskErrors}
     * @param failedAt when the attempt failed, in Unix milliseconds
     */
    @JsonCreator
    public JobError(
            @JsonProperty("id") String id,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("attempt") int attempt,
            @JsonProperty("error") String error,
            @JsonProperty("failedAt") long failedAt) {
        this.id = id;
        this.jobId = jobId;
        this.attempt = attempt;
        this.error = error;
        this.failedAt = failedAt;
    }
}
