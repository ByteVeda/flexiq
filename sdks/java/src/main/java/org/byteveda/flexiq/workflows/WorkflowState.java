package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

/** State of a workflow run. Wire form is the lowercase name, shared across SDKs. */
public enum WorkflowState {
    /** Submitted; no node has started yet. */
    PENDING,
    /** At least one node is running or ready. */
    RUNNING,
    /** Held: nothing new is dispatched until it resumes. */
    PAUSED,
    /** Every node completed. */
    COMPLETED,
    /** The run finished, but some nodes failed or were skipped. */
    COMPLETED_WITH_FAILURES,
    /** The run failed. */
    FAILED,
    /** The run was cancelled before it finished. */
    CANCELLED,
    /** A failure is being rolled back, node by node in reverse order. */
    COMPENSATING,
    /** The rollback finished cleanly. */
    COMPENSATED,
    /** The rollback itself failed; the run needs a human. */
    COMPENSATION_FAILED;

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
    public static WorkflowState fromWire(String wire) {
        return valueOf(wire.toUpperCase(Locale.ROOT));
    }

    /**
     * Whether the run has reached a final state (no further transitions).
     *
     * @return {@code true} for every state a run never leaves
     */
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
