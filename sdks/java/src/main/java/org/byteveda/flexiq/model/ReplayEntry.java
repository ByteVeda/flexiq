package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One entry in a job's replay history. {@code replayedAt} is Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ReplayEntry {
    public final String id;
    public final String originalJobId;
    public final String replayJobId;
    public final long replayedAt;
    public final String originalError;
    public final String replayError;

    @JsonCreator
    public ReplayEntry(
            @JsonProperty("id") String id,
            @JsonProperty("originalJobId") String originalJobId,
            @JsonProperty("replayJobId") String replayJobId,
            @JsonProperty("replayedAt") long replayedAt,
            @JsonProperty("originalError") String originalError,
            @JsonProperty("replayError") String replayError) {
        this.id = id;
        this.originalJobId = originalJobId;
        this.replayJobId = replayJobId;
        this.replayedAt = replayedAt;
        this.originalError = originalError;
        this.replayError = replayError;
    }
}
