package org.byteveda.flexiq.dashboard.store;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.dashboard.support.DashboardError;
import org.byteveda.flexiq.dashboard.support.Json;
import org.byteveda.flexiq.internal.SettingsDocument;
import org.jspecify.annotations.Nullable;

/**
 * Per-task and per-queue runtime overrides, persisted in the settings KV at
 * {@code overrides:task:<name>} / {@code overrides:queue:<name>}. Rows are
 * normalised (every field present, unset fields {@code null}) and stamped with
 * {@code updated_at} in milliseconds. A {@code null} field in a PUT patch clears
 * it; absent fields are left unchanged.
 */
public final class OverridesStore {
    /** Prefix of the per-task override keys. */
    public static final String TASK_PREFIX = "overrides:task:";

    /** Prefix of the per-queue override keys. */
    public static final String QUEUE_PREFIX = "overrides:queue:";

    private static final List<String> TASK_FIELDS =
            List.of("rate_limit", "max_concurrent", "max_retries", "retry_backoff", "timeout", "priority", "paused");
    private static final List<String> QUEUE_FIELDS = List.of("rate_limit", "max_concurrent", "paused");

    private static final SettingsDocument.Codec<Map<String, Object>> ROW_CODEC =
            SettingsDocument.codec(OverridesStore::decodeRow, Json::toString);

    private final SettingsAccess settings;

    /**
     * A store over one queue's settings documents.
     *
     * @param settings where the override rows live
     */
    public OverridesStore(SettingsAccess settings) {
        this.settings = settings;
    }

    /**
     * One task's override row.
     *
     * @param name the task's name
     * @return the normalised row, or {@code null} when none is stored
     */
    public @Nullable Map<String, Object> getTask(String name) {
        return read(TASK_PREFIX + name);
    }

    /**
     * One queue's override row.
     *
     * @param name the queue's name
     * @return the normalised row, or {@code null} when none is stored
     */
    public @Nullable Map<String, Object> getQueue(String name) {
        return read(QUEUE_PREFIX + name);
    }

    /**
     * Merge a patch into one task's override row.
     *
     * @param name the task's name
     * @param patch the fields to set; an explicit {@code null} clears a field, an
     *     absent one leaves it alone
     * @return the row as it stands after the write
     */
    public Map<String, Object> putTask(String name, Map<String, Object> patch) {
        return put(TASK_PREFIX + name, "task_name", name, TASK_FIELDS, patch);
    }

    /**
     * Merge a patch into one queue's override row.
     *
     * @param name the queue's name
     * @param patch the fields to set; an explicit {@code null} clears a field, an
     *     absent one leaves it alone
     * @return the row as it stands after the write
     */
    public Map<String, Object> putQueue(String name, Map<String, Object> patch) {
        return put(QUEUE_PREFIX + name, "queue_name", name, QUEUE_FIELDS, patch);
    }

    /**
     * Drop one task's override row.
     *
     * @param name the task's name
     * @return whether a row existed
     */
    public boolean deleteTask(String name) {
        return settings.deleteSetting(TASK_PREFIX + name);
    }

    /**
     * Drop one queue's override row.
     *
     * @param name the queue's name
     * @return whether a row existed
     */
    public boolean deleteQueue(String name) {
        return settings.deleteSetting(QUEUE_PREFIX + name);
    }

    /**
     * Task names that carry an override row.
     *
     * @return the names, sorted
     */
    public java.util.Set<String> taskNames() {
        return names(TASK_PREFIX);
    }

    /**
     * Queue names that carry an override row.
     *
     * @return the names, sorted
     */
    public java.util.Set<String> queueNames() {
        return names(QUEUE_PREFIX);
    }

    private java.util.Set<String> names(String prefix) {
        java.util.Set<String> out = new java.util.TreeSet<>();
        for (String key : settings.listSettings().keySet()) {
            if (key.startsWith(prefix)) {
                out.add(key.substring(prefix.length()));
            }
        }
        return out;
    }

    private @Nullable Map<String, Object> read(String key) {
        return settings.getSetting(key).map(Json::parseMap).orElse(null);
    }

    private static Map<String, Object> decodeRow(java.util.Optional<String> raw) {
        Map<String, Object> parsed = raw.map(Json::parseMap).orElse(null);
        return parsed != null ? new LinkedHashMap<>(parsed) : new LinkedHashMap<>();
    }

    /** Patch {@code patch} into the override at {@code key} without losing a concurrent edit. */
    private Map<String, Object> put(
            String key, String nameKey, String name, List<String> fields, Map<String, Object> patch) {
        return SettingsDocument.update(settings, key, ROW_CODEC, stored -> {
            Map<String, Object> values = new LinkedHashMap<>();
            for (String field : fields) {
                if (stored.get(field) != null) {
                    values.put(field, stored.get(field));
                }
            }
            for (Map.Entry<String, Object> entry : patch.entrySet()) {
                String field = entry.getKey();
                if (!fields.contains(field)) {
                    throw DashboardError.badRequest("unknown override field: " + field);
                }
                Object value = entry.getValue();
                validate(field, value);
                if (value == null) {
                    values.remove(field);
                } else {
                    values.put(field, value);
                }
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put(nameKey, name);
            for (String field : fields) {
                row.put(field, field.equals("paused") ? Boolean.TRUE.equals(values.get("paused")) : values.get(field));
            }
            row.put("updated_at", System.currentTimeMillis());
            stored.clear();
            stored.putAll(row);
            return row;
        });
    }

    private static void validate(String field, Object value) {
        if (value == null) {
            return; // clears the field
        }
        switch (field) {
            case "rate_limit" -> {
                if (!(value instanceof String s) || s.isEmpty() || !s.contains("/")) {
                    throw DashboardError.badRequest("rate_limit must look like '<count>/<period>'");
                }
            }
            case "max_concurrent", "max_retries" -> requireIntegral(field, value, 0);
            case "timeout" -> requireIntegral(field, value, 1);
            case "priority" -> requireIntegral(field, value, Long.MIN_VALUE);
            case "retry_backoff" -> {
                if (!(value instanceof Number n) || n.doubleValue() < 0) {
                    throw DashboardError.badRequest("retry_backoff must be a non-negative number");
                }
            }
            case "paused" -> {
                if (!(value instanceof Boolean)) {
                    throw DashboardError.badRequest("paused must be a boolean");
                }
            }
            default -> throw DashboardError.badRequest("unknown override field: " + field);
        }
    }

    private static void requireIntegral(String field, Object value, long min) {
        if (!(value instanceof Number number) || number.doubleValue() != Math.floor(number.doubleValue())) {
            throw DashboardError.badRequest(field + " must be an integer");
        }
        if (number.longValue() < min) {
            throw DashboardError.badRequest(field + " must be >= " + min);
        }
    }
}
