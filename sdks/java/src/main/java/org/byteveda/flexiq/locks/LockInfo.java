package org.byteveda.flexiq.locks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Current holder of a distributed lock. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class LockInfo {
    /** The lock's name, shared by everyone contending for it. */
    public final String lockName;

    /** The holder's per-instance id, which release and extend are scoped to. */
    public final String ownerId;

    /** When the current holder took it, in Unix milliseconds. */
    public final long acquiredAt;

    /** When it lapses without an extend, in Unix milliseconds. */
    public final long expiresAt;

    /**
     * A holder snapshot, decoded from the backend's lock row.
     *
     * @param lockName the lock's name
     * @param ownerId the holder's per-instance id
     * @param acquiredAt when the holder took it, in Unix milliseconds
     * @param expiresAt when it lapses without an extend, in Unix milliseconds
     */
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
