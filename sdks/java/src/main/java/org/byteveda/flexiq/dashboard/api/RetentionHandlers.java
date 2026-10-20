package org.byteveda.flexiq.dashboard.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.support.DashboardError;
import org.byteveda.flexiq.model.EffectiveRetention;
import org.byteveda.flexiq.model.RetentionSource;
import org.jspecify.annotations.Nullable;

/**
 * Echoes the retention windows the elected cleaner published for this
 * namespace, so the dashboard can explain why rows disappear from its
 * listings, and stores the dashboard's override of the worker's config.
 * Retention runs in the worker process, so the echo is never computed from
 * local config — an unreported policy is reported as such.
 */
public final class RetentionHandlers {

    private static final Set<String> TABLES = Set.of(
            "task_logs_ttl_ms",
            "archived_jobs_ttl_ms",
            "job_errors_ttl_ms",
            "task_metrics_ttl_ms",
            "dead_letter_ttl_ms");

    private final FlexiQ queue;

    /**
     * Handlers reading one queue's retention policy.
     *
     * @param queue what the routes below read from
     */
    public RetentionHandlers(FlexiQ queue) {
        this.queue = queue;
    }

    /**
     * The published retention policy for this queue's namespace.
     *
     * @return the policy, or a null body when no cleaner has published one
     */
    public Object retention() {
        return Contract.retention(queue.effectiveRetention().orElse(null));
    }

    /** Preview what a purge would delete under the policy the next sweep
     *  applies, computed in-process — so it always answers.
     *
     * @return the counts a purge would delete, per class of record
     */
    public Object retentionDryRun() {
        return Contract.retentionDryRun(queue.dryRunRetention());
    }

    /**
     * The stored override, or the {@code code} default when none was saved.
     *
     * @return the override
     */
    public Object retentionOverride() {
        return Contract.retentionOverride(queue.retentionOverride());
    }

    /**
     * Store the dashboard's windows and which source applies. The body is
     * checked here for shape; the windows themselves are validated by the core,
     * whose refusal is answered as a 400 like this one.
     *
     * @param body {@code {source, windows}}
     * @return the stored override
     */
    public Object putRetentionOverride(Map<String, Object> body) {
        Object source = body.get("source");
        if (!"code".equals(source) && !"dashboard".equals(source)) {
            throw DashboardError.badRequest("source must be 'code' or 'dashboard'");
        }
        Map<String, @Nullable Long> windows = new HashMap<>();
        Object raw = body.getOrDefault("windows", Map.of());
        if (!(raw instanceof Map<?, ?> fields)) {
            throw DashboardError.badRequest("windows must be an object");
        }
        for (Map.Entry<?, ?> field : fields.entrySet()) {
            String name = String.valueOf(field.getKey());
            if (!TABLES.contains(name)) {
                throw DashboardError.badRequest("unknown retention table '" + name + "'");
            }
            Object value = field.getValue();
            // Jackson decodes fractions as Double and oversized integers as BigInteger;
            // longValue() would truncate or wrap them where the core refuses both.
            if (value != null && !(value instanceof Integer || value instanceof Long)) {
                throw DashboardError.badRequest(name + " must be a whole number of milliseconds or null");
            }
            windows.put(name, value == null ? null : ((Number) value).longValue());
        }
        try {
            return Contract.retentionOverride(queue.setRetentionOverride(
                    RetentionSource.fromWire((String) source),
                    new EffectiveRetention.Windows(
                            windows.get("archived_jobs_ttl_ms"),
                            windows.get("dead_letter_ttl_ms"),
                            windows.get("task_logs_ttl_ms"),
                            windows.get("task_metrics_ttl_ms"),
                            windows.get("job_errors_ttl_ms"))));
        } catch (IllegalArgumentException e) {
            // The core's refusal of the windows, raised before anything is written.
            throw DashboardError.badRequest(String.valueOf(e.getMessage()));
        }
    }

    /**
     * Forget the override; the worker's config applies from the next sweep.
     *
     * @return whether one existed, under {@code deleted}
     */
    public Object deleteRetentionOverride() {
        return Map.of("deleted", queue.clearRetentionOverride());
    }
}
