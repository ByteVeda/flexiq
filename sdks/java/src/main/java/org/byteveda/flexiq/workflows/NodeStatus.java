package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

/** Status of a single node within a workflow run. Wire form is the lowercase name. */
public enum NodeStatus {
    PENDING,
    READY,
    RUNNING,
    COMPLETED,
    FAILED,
    SKIPPED,
    WAITING_APPROVAL,
    CACHE_HIT,
    COMPENSATING,
    COMPENSATED,
    COMPENSATION_FAILED;

    @JsonValue
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    @JsonCreator
    public static NodeStatus fromWire(String wire) {
        return valueOf(wire.toUpperCase(Locale.ROOT));
    }
}
