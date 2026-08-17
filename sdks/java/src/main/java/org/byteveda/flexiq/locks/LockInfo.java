package org.byteveda.flexiq.locks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Current holder of a distributed lock. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class LockInfo {
    public final String lockName;
    public final String ownerId;
    public final long acquiredAt;
    public final long expiresAt;

    @JsonCreator
    public LockInfo(
            @JsonProperty("lockName") String lockName,
            @JsonProperty("ownerId") String ownerId,
            @JsonProperty("acquiredAt") long acquiredAt,
            @JsonProperty("expiresAt") long expiresAt) {
        this.lockName = lockName;
        this.ownerId = ownerId;
        this.acquiredAt = acquiredAt;
        this.expiresAt = expiresAt;
    }
}
