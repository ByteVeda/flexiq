package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One recorded error attempt for a job. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobError {
    public final String id;
    public final String jobId;
    public final int attempt;
    public final String error;
    public final long failedAt;

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
