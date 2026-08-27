package org.byteveda.flexiq.health;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One worker resource as seen across every live worker's heartbeat. A resource
 * a worker advertises but has never reported on is {@code not_initialized}.
 *
 * @param name the resource's registered name
 * @param scope how long one instance lives — {@code worker} or {@code task}
 * @param health the worst health any live worker reported, or {@code not_initialized}
 * @param initDurationMs how long the slowest reported initialisation took
 * @param recreations how many times a worker rebuilt it after a failed health check
 * @param dependsOn names of the resources built before this one
 */
public record ResourceStatusEntry(
        String name, String scope, String health, long initDurationMs, int recreations, List<String> dependsOn) {

    /**
     * The snake_case row the dashboard and the probe endpoints report.
     *
     * @return the row, keyed as the cross-SDK contract spells it
     */
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
