package org.byteveda.flexiq.model;

import java.util.Locale;
import org.byteveda.flexiq.errors.SerializationException;

/**
 * Order in which same-priority jobs leave a queue. Priority always dominates; this only
 * breaks ties. Wire form is the lowercase name, shared across SDKs.
 */
public enum DispatchOrder {
    /** Oldest-first — the fair default a durable queue is expected to keep. */
    FIFO,
    /** Newest-first — a freshness lever for same-priority ties under load. */
    LIFO;

    /**
     * Lowercase wire form passed to the native layer.
     *
     * @return the wire form
     */
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Parse a wire form ({@code "fifo"}/{@code "lifo"}).
     *
     * @param wire the value the core reported
     * @return the matching constant
     */
    public static DispatchOrder fromWire(String wire) {
        if (wire == null) {
            throw new SerializationException("dispatch order is null");
        }
        try {
            return valueOf(wire.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new SerializationException("unknown dispatch order: " + wire, e);
        }
    }
}
