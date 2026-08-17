package org.byteveda.flexiq.model;

import java.util.Locale;
import org.byteveda.flexiq.errors.SerializationException;

/**
 * Storage backend a {@link org.byteveda.flexiq.FlexiQ} client opens. The wire form is the
 * lowercase name, shared across SDKs.
 */
public enum StorageBackend {
    /** Brokerless SQLite file store — the default. */
    SQLITE,
    /** PostgreSQL. */
    POSTGRES,
    /** Redis. */
    REDIS;

    /** Lowercase wire form passed to the native layer. */
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Parse a wire form ({@code "sqlite"}/{@code "postgres"}/{@code "redis"}). */
    public static StorageBackend fromWire(String wire) {
        if (wire == null) {
            throw new SerializationException("storage backend is null");
        }
        try {
            return valueOf(wire.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new SerializationException("unknown storage backend: " + wire, e);
        }
    }
}
