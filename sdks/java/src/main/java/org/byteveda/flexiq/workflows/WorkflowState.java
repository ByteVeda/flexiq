package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

/** State of a workflow run. Wire form is the lowercase name, shared across SDKs. */
public enum WorkflowState {
    PENDING,
    RUNNING,
    PAUSED,
    COMPLETED,
    COMPLETED_WITH_FAILURES,
    FAILED,
    CANCELLED,
    COMPENSATING,
    COMPENSATED,
    COMPENSATION_FAILED;

    @JsonValue
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    @JsonCreator
    public static WorkflowState fromWire(String wire) {
        return valueOf(wire.toUpperCase(Locale.ROOT));
    }

    /** Whether the run has reached a final state (no further transitions). */
    public boolean isTerminal() {
        switch (this) {
            case COMPLETED:
            case COMPLETED_WITH_FAILURES:
            case FAILED:
            case CANCELLED:
            case COMPENSATED:
            case COMPENSATION_FAILED:
                return true;
            default:
                return false;
        }
    }
}
