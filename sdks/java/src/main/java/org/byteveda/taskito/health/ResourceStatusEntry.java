package org.byteveda.taskito.health;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One worker resource as seen across every live worker's heartbeat. A resource
 * a worker advertises but has never reported on is {@code not_initialized}.
 */
public record ResourceStatusEntry(
        String name, String scope, String health, long initDurationMs, int recreations, List<String> dependsOn) {

    /** The snake_case row the dashboard and the probe endpoints report. */
    public Map<String, Object> toMap() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", name);
        row.put("scope", scope);
        row.put("health", health);
        row.put("init_duration_ms", initDurationMs);
        row.put("recreations", recreations);
        row.put("depends_on", dependsOn);
        return row;
    }
}
