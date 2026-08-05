package org.byteveda.taskito.health;

import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Readiness payload: {@code ready} only when every check passed, {@code degraded}
 * otherwise. {@code resources} is absent when no worker advertises any.
 */
public record ReadinessReport(String status, String storage, WorkersCheck workers, @Nullable ResourcesCheck resources) {

    /** Whether every dependency answered healthily. */
    public boolean ready() {
        return "ready".equals(status);
    }

    /** The probe body, in the shape every shell reports. */
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
