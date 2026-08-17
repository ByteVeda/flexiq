package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import org.byteveda.flexiq.errors.SerializationException;

/**
 * Severity of a structured task log. Distinct from the SDK's own logger level: this one
 * is written to storage and read back by every SDK, so its wire form — the lowercase
 * name — is part of the cross-SDK contract.
 */
public enum TaskLogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL,
    /** Partial results published from a running task, not a severity. */
    RESULT;

    @JsonValue
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    @JsonCreator
    public static TaskLogLevel fromWire(String wire) {
        if (wire == null) {
            throw new SerializationException("task log level is null");
        }
        try {
            return valueOf(wire.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new SerializationException("unknown task log level: " + wire, e);
        }
    }
}
