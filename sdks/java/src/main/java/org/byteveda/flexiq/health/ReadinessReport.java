package org.byteveda.flexiq.health;

import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Readiness payload: {@code ready} when storage answered and no advertised resource
 * is unhealthy, {@code degraded} otherwise. {@code resources} is absent when no
 * worker advertises any.
 *
 * <p><b>The worker count does not enter into it.</b> A queue with nothing running
 * against it reports {@code ready} with a worker check of {@code none}; only a
 * worker lookup that <i>failed</i> degrades the status.
 *
 * @param status {@code ready} when storage answered and no advertised resource is
 *     unhealthy, else {@code degraded}
 * @param storage the storage check's own status
 * @param workers the worker check
 * @param resources the worker-resource check, or {@code null} when no worker advertises any
 */
public record ReadinessReport(String status, String storage, WorkersCheck workers, @Nullable ResourcesCheck resources) {

    /**
     * Whether every dependency answered healthily.
     *
     * @return {@code true} when the status is {@code ready}
     */
    public boolean ready() {
        return "ready".equals(status);
    }

    /**
     * The probe body, in the shape every shell reports.
     *
     * @return the status plus a {@code checks} map, resources omitted when absent
     */
    public Map<String, Object> toMap() {
        Map<String, Object> checks = new LinkedHashMap<>();
        checks.put("storage", storage);
        checks.put("workers", workers.toWire());
        if (resources != null) {
            checks.put("resources", resources.toWire());
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status);
        body.put("checks", checks);
        return body;
    }
}
