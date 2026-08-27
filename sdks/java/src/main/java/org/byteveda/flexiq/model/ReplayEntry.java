package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One entry in a job's replay history. {@code replayedAt} is Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ReplayEntry {
    /** The replay row's own id. */
    public final String id;

    /** The job whose payload was re-run. */
    public final String originalJobId;

    /** The job the replay minted. */
    public final String replayJobId;

    /** When the replay was requested, in Unix milliseconds. */
    public final long replayedAt;

    /** What the original job failed with, or {@code null}. */
    public final String originalError;

    /** What the replay failed with, or {@code null} while it is still live or if it succeeded. */
    public final String replayError;

    /**
     * Decoded from the core's JSON replay row.
     *
     * @param id the replay row's own id
     * @param originalJobId the job whose payload was re-run
     * @param replayJobId the job the replay minted
     * @param replayedAt when the replay was requested, in Unix milliseconds
     * @param originalError what the original job failed with, or {@code null}
     * @param replayError what the replay failed with, or {@code null} while it is still live or if it succeeded
     */
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
