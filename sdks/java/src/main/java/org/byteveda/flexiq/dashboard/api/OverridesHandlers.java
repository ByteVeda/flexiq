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

    public OverridesHandlers(FlexiQ queue, OverridesStore store) {
        this.queue = queue;
        this.store = store;
    }

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

    public Object listQueues() {
        TreeSet<String> names = new TreeSet<>(queue.statsAllQueues().keySet());
        names.addAll(queue.listPausedQueues());
        names.addAll(store.queueNames());
        return new ArrayList<>(names);
    }

    public @Nullable Object getTaskOverride(String name) {
        return store.getTask(name);
    }

    public Object putTaskOverride(String name, Map<String, Object> body) {
        return store.putTask(name, body);
    }

    public Object deleteTaskOverride(String name) {
        return Map.of("cleared", store.deleteTask(name));
    }

    public @Nullable Object getQueueOverride(String name) {
        return store.getQueue(name);
    }

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

    public Object deleteQueueOverride(String name) {
        return Map.of("cleared", store.deleteQueue(name));
    }
}
