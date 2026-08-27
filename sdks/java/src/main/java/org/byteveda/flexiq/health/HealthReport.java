package org.byteveda.flexiq.health;

import java.util.Map;

/** Liveness payload: the process answered, nothing else is asserted. *
 * @param status the liveness status; {@link Health#check()} always reports {@code ok},
 *     since a process that could not answer would never reach it. The constructor
 *     itself accepts any value.
 */
public record HealthReport(String status) {

    /**
     * The probe body, in the shape every shell reports.
     *
     * @return a single-key map holding the status
     */
    public Map<String, Object> toMap() {
        return Map.of("status", status);
    }
}
