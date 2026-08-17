package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import org.byteveda.flexiq.errors.SerializationException;

/** Lifecycle state of a job. Wire form is the lowercase name, shared across SDKs. */
public enum JobStatus {
    PENDING,
    RUNNING,
    COMPLETE,
    FAILED,
    DEAD,
    CANCELLED;

    @JsonValue
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

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
