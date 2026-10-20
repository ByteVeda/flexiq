package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.jspecify.annotations.Nullable;

/** Which retention windows the elected cleaner applies. */
public enum RetentionSource {
    /** The windows the worker was configured with in code. */
    CODE("code"),
    /** The windows stored from a dashboard. */
    DASHBOARD("dashboard");

    private final String wire;

    RetentionSource(String wire) {
        this.wire = wire;
    }

    /**
     * The name on the wire.
     *
     * @return {@code "code"} or {@code "dashboard"}
     */
    @JsonValue
    public String wire() {
        return wire;
    }

    /**
     * Decode a wire name. Anything unrecognised reads as {@link #CODE}: a
     * document from a newer worker must not stop this one reading the rest.
     *
     * @param wire the name on the wire, possibly {@code null}
     * @return the source it names
     */
    @JsonCreator
    public static RetentionSource fromWire(@Nullable String wire) {
        return "dashboard".equals(wire) ? DASHBOARD : CODE;
    }
}
