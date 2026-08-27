package org.byteveda.flexiq.workflows;

import java.util.Locale;
import org.byteveda.flexiq.errors.SerializationException;

/**
 * When a step runs, based on its predecessors' outcomes. The wire form is the lowercase
 * snake_case name, shared across SDKs.
 */
public enum WorkflowCondition {
    /** Every predecessor completed — the default. */
    ON_SUCCESS,
    /** At least one predecessor failed — an error-handler branch. */
    ON_FAILURE,
    /** Regardless of predecessor outcomes, once they settle. */
    ALWAYS;

    /**
     * Lowercase snake_case wire form ({@code "on_success"}/{@code "on_failure"}/{@code "always"}).
     *
     * @return the wire form
     */
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Parse a wire form ({@code "on_success"}/{@code "on_failure"}/{@code "always"}).
     *
     * @param wire the stored value
     * @return the matching constant
     */
    public static WorkflowCondition fromWire(String wire) {
        if (wire == null) {
            throw new SerializationException("workflow condition is null");
        }
        try {
            return valueOf(wire.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new SerializationException("unknown workflow condition: " + wire, e);
        }
    }
}
