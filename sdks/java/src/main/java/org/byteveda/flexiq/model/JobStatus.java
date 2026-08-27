package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import org.byteveda.flexiq.errors.SerializationException;

/** Lifecycle state of a job. Wire form is the lowercase name, shared across SDKs. */
public enum JobStatus {
    /** Waiting to be claimed. */
    PENDING,
    /** A worker holds a claim and the handler is running. */
    RUNNING,
    /** Finished successfully. */
    COMPLETE,
    /** The last attempt failed; the job will be tried again. */
    FAILED,
    /** Retries exhausted; the job is in the dead-letter queue. */
    DEAD,
    /** Cancelled before it reached a verdict. */
    CANCELLED;

    /**
     * Lowercase wire form shared across SDKs.
     *
     * @return the wire form
     */
    @JsonValue
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Parse a wire form.
     *
     * @param wire the value the core reported
     * @return the matching constant
     */
    @JsonCreator
    public static JobStatus fromWire(String wire) {
        if (wire == null) {
            throw new SerializationException("job status is null");
        }
        try {
            return valueOf(wire.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new SerializationException("unknown job status: " + wire, e);
        }
    }
}
