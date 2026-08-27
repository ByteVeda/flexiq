package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

/** Status of a single node within a workflow run. Wire form is the lowercase name. */
public enum NodeStatus {
    /** Waiting on a predecessor. */
    PENDING,
    /** Predecessors settled; the node's job is enqueued. */
    READY,
    /** A worker is running the node's job. */
    RUNNING,
    /** The node finished successfully. */
    COMPLETED,
    /** The node's job exhausted its retries. */
    FAILED,
    /** The node's condition did not hold, so it never ran. */
    SKIPPED,
    /** A gate node parked, awaiting a decision. */
    WAITING_APPROVAL,
    /** A cached node the worker skipped; it produces no forward result. */
    CACHE_HIT,
    /** The node's compensation job is running. */
    COMPENSATING,
    /** The node was rolled back cleanly. */
    COMPENSATED,
    /** The node's compensation itself failed. */
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
    public static NodeStatus fromWire(String wire) {
        return valueOf(wire.toUpperCase(Locale.ROOT));
    }
}
