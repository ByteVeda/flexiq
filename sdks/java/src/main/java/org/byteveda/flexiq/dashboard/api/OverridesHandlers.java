package org.byteveda.flexiq.dashboard.api;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeSet;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.store.OverridesStore;
import org.byteveda.flexiq.model.CircuitBreakerState;
import org.byteveda.flexiq.model.TaskMetric;
import org.jspecify.annotations.Nullable;

/**
 * Task/queue override CRUD plus the task/queue listings the overrides UI selects
 * from. The Java SDK has no client-side task/queue registry, so listings are
 * derived from observable state (metrics, circuit breakers, live queues, and
 * existing override rows) — a best-effort superset rather than a declared registry.
 * Setting a queue's {@code paused} override also pauses/resumes it live.
 */
public final class OverridesHandlers {
    private final FlexiQ queue;
    private final OverridesStore store;

    /**
     * Handlers over one queue's overrides.
     *
     * @param queue read for the observable task and queue names, and paused live
     *     when a queue override says so
     * @param store where the override rows are persisted
     */
    public OverridesHandlers(FlexiQ queue, OverridesStore store) {
        this.queue = queue;
        this.store = store;
    }

    /**
     * Task names the overrides UI can select from.
     *
     * @return every task seen in metrics, circuit breakers or an existing override
     *     row — a best-effort superset, since this SDK has no declared registry
     */
    public Object listTasks() {
        TreeSet<String> names = new TreeSet<>();
        for (TaskMetric metric : queue.metrics(null, 0)) {
            names.add(metric.taskName);
        }
        for (CircuitBreakerState breaker : queue.listCircuitBreakers()) {
            names.add(breaker.taskName);
        }
        names.addAll(store.taskNames());
        return new ArrayList<>(names);
    }

    /**
     * Queue names the overrides UI can select from.
     *
     * @return every queue with stats, paused, or carrying an override row
     */
    public Object listQueues() {
        TreeSet<String> names = new TreeSet<>(queue.statsAllQueues().keySet());
        names.addAll(queue.listPausedQueues());
        names.addAll(store.queueNames());
        return new ArrayList<>(names);
    }

    /**
     * One task's override row.
     *
     * @param name the task's name
     * @return the row, or {@code null} for a 404
     */
    public @Nullable Object getTaskOverride(String name) {
        return store.getTask(name);
    }

    /**
     * Merge fields into one task's override row.
     *
     * @param name the task's name
     * @param body the fields to set
     * @return the row as it stands after the write
     */
    public Object putTaskOverride(String name, Map<String, Object> body) {
        return store.putTask(name, body);
    }

    /**
     * Drop one task's override row, so the task falls back to its declared settings.
     *
     * @param name the task's name
     * @return whether a row was removed, under {@code cleared}
     */
    public Object deleteTaskOverride(String name) {
        return Map.of("cleared", store.deleteTask(name));
    }

    /**
     * One queue's override row.
     *
     * @param name the queue's name
     * @return the row, or {@code null} for a 404
     */
    public @Nullable Object getQueueOverride(String name) {
        return store.getQueue(name);
    }

    /**
     * Merge fields into one queue's override row, reconciling {@code paused} live.
     *
     * @param name the queue's name
     * @param body the fields to set; touching {@code paused} pauses or resumes the
     *     queue to match the resulting row, clearing included
     * @return the row as it stands after the write
     */
    public Object putQueueOverride(String name, Map<String, Object> body) {
        Map<String, Object> row = store.putQueue(name, body);
        // Reconcile the live queue to the resulting override whenever the caller
        // touches `paused` — including clearing it (which resolves to not-paused),
        // where reading the request value alone would leave the queue paused.
        if (body.containsKey("paused")) {
            if (Boolean.TRUE.equals(row.get("paused"))) {
                queue.queue(name).pause();
            } else {
                queue.queue(name).resume();
            }
        }
        return row;
    }

    /**
     * Drop one queue's override row.
     *
     * @param name the queue's name
     * @return whether a row was removed, under {@code cleared}
     */
    public Object deleteQueueOverride(String name) {
        return Map.of("cleared", store.deleteQueue(name));
    }
}
