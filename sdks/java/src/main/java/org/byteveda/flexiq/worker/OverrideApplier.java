package org.byteveda.flexiq.worker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.byteveda.flexiq.dashboard.store.OverridesStore;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.jspecify.annotations.Nullable;

/**
 * Folds stored task and queue overrides into a worker's wire configs at start
 * (cross-SDK contract). A running worker never re-reads them; only a start does.
 *
 * <p>A malformed stored field is skipped with a warning rather than failing the
 * start: an operator's bad edit must not take the fleet down on its next deploy.
 */
final class OverrideApplier {
    private static final FlexiQLogger LOG = FlexiQLogger.create("worker");

    /** The count half of a rate spec; the core parses the same decimal grammar. */
    private static final Pattern RATE_COUNT = Pattern.compile("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?");

    private static final List<String> RATE_UNITS =
            List.of("s", "sec", "second", "m", "min", "minute", "h", "hr", "hour");

    private OverrideApplier() {}

    /**
     * Patch task configs with stored task overrides, for registered tasks only.
     * {@code rate_limit}, {@code max_concurrent} and {@code retry_backoff} (seconds,
     * becoming the retry base delay in ms) have a slot; {@code max_retries} travels
     * per job on enqueue, and {@code timeout}, {@code priority} and {@code paused}
     * are enforced elsewhere, so none of those change a task config here. A task
     * with an override but no declared policy gains a fresh entry.
     *
     * @param configs the declared task configs in wire shape; not mutated
     * @param registered every task name the worker registered a handler for
     * @param store the overrides of the worker's namespace
     * @return the patched configs, declared order first, then new entries by name
     */
    static List<Map<String, Object>> applyTaskOverrides(
            List<Map<String, Object>> configs, Collection<String> registered, OverridesStore store) {
        Map<String, Map<String, Object>> byName = indexByName(configs);
        for (String name : store.taskNames()) {
            if (!registered.contains(name)) {
                continue;
            }
            Map<String, Object> row = store.getTask(name);
            if (row == null) {
                LOG.warn("ignoring the override of task '" + name + "': the stored row is not a JSON object");
                continue;
            }
            Map<String, Object> patch = new LinkedHashMap<>();
            String subject = "task '" + name + "'";
            putIfValid(patch, "rateLimit", rateLimit(subject, row.get("rate_limit")));
            putIfValid(patch, "maxConcurrent", maxConcurrent(subject, row.get("max_concurrent")));
            putIfValid(patch, "baseDelayMs", retryBackoffMs(subject, row.get("retry_backoff")));
            merge(byName, name, patch);
        }
        return new ArrayList<>(byName.values());
    }

    /**
     * Merge every stored queue override's {@code rate_limit} and {@code max_concurrent}
     * into the queue config of the same name, adding the queue when absent.
     * {@code paused} is enforced elsewhere and changes no queue config here.
     *
     * @param configs the declared queue configs in wire shape; not mutated
     * @param store the overrides of the worker's namespace
     * @return the merged configs, declared order first, then new queues by name
     */
    static List<Map<String, Object>> applyQueueOverrides(List<Map<String, Object>> configs, OverridesStore store) {
        Map<String, Map<String, Object>> byName = indexByName(configs);
        for (String name : store.queueNames()) {
            Map<String, Object> row = store.getQueue(name);
            if (row == null) {
                LOG.warn("ignoring the override of queue '" + name + "': the stored row is not a JSON object");
                continue;
            }
            Map<String, Object> patch = new LinkedHashMap<>();
            String subject = "queue '" + name + "'";
            putIfValid(patch, "rateLimit", rateLimit(subject, row.get("rate_limit")));
            putIfValid(patch, "maxConcurrent", maxConcurrent(subject, row.get("max_concurrent")));
            merge(byName, name, patch);
        }
        return new ArrayList<>(byName.values());
    }

    /** Copy each config so the caller's maps stay untouched, keyed by its {@code name}. */
    private static Map<String, Map<String, Object>> indexByName(List<Map<String, Object>> configs) {
        Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
        for (Map<String, Object> config : configs) {
            byName.put(String.valueOf(config.get("name")), new LinkedHashMap<>(config));
        }
        return byName;
    }

    /** Apply {@code patch}; an override that sets nothing adds no empty entry. */
    private static void merge(Map<String, Map<String, Object>> byName, String name, Map<String, Object> patch) {
        if (patch.isEmpty()) {
            return;
        }
        byName.computeIfAbsent(name, OverrideApplier::freshConfig).putAll(patch);
    }

    private static Map<String, Object> freshConfig(String name) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("name", name);
        return config;
    }

    private static void putIfValid(Map<String, Object> patch, String wireKey, @Nullable Object value) {
        if (value != null) {
            patch.put(wireKey, value);
        }
    }

    /** A rate spec the core accepts, else {@code null}: the core fails a start on one it cannot read. */
    private static @Nullable String rateLimit(String subject, @Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String spec && isValidRate(spec)) {
            return spec;
        }
        return skip(subject, "rate_limit", value);
    }

    /** Same rule as the core's rate parser: {@code <count>/<unit>}, a finite count of at least one. */
    static boolean isValidRate(String spec) {
        String[] parts = spec.split("/", -1);
        if (parts.length != 2) {
            return false;
        }
        String count = parts[0].strip();
        if (!RATE_COUNT.matcher(count).matches()) {
            return false;
        }
        double parsed = Double.parseDouble(count);
        return Double.isFinite(parsed) && parsed >= 1.0 && RATE_UNITS.contains(parts[1].strip());
    }

    /** A non-negative integer that fits the binding's 32-bit slot, else {@code null}. */
    private static @Nullable Integer maxConcurrent(String subject, @Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            double d = number.doubleValue();
            if (d == Math.floor(d) && d >= 0 && d <= Integer.MAX_VALUE) {
                return (int) d;
            }
        }
        return skip(subject, "max_concurrent", value);
    }

    /** Seconds as a non-negative finite number, returned as whole milliseconds. */
    private static @Nullable Long retryBackoffMs(String subject, @Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            double seconds = number.doubleValue();
            if (Double.isFinite(seconds) && seconds >= 0) {
                return Math.round(seconds * 1000);
            }
        }
        return skip(subject, "retry_backoff", value);
    }

    private static <T> @Nullable T skip(String subject, String field, Object value) {
        LOG.warn("ignoring " + field + " in the override of " + subject + ": malformed stored value '" + value + "'");
        return null;
    }
}
