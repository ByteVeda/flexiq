package org.byteveda.flexiq.health;

import java.util.Map;

/** Liveness payload: the process answered, nothing else is asserted. *
 * @param status always {@code ok} — a process that could not answer does not reach this
 */
public record HealthReport(String status) {

    /** The probe body, in the shape every shell reports. */
    public Map<String, Object> toMap() {
        return Map.of("status", status);
    }
}
