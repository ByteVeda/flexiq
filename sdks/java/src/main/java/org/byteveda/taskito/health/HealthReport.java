package org.byteveda.taskito.health;

import java.util.Map;

/** Liveness payload: the process answered, nothing else is asserted. */
public record HealthReport(String status) {

    /** The probe body, in the shape every shell reports. */
    public Map<String, Object> toMap() {
        return Map.of("status", status);
    }
}
