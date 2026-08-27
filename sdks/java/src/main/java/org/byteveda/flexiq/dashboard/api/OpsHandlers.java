package org.byteveda.flexiq.dashboard.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.support.Http;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.health.Health;
import org.byteveda.flexiq.health.ResourceStatusEntry;
import org.byteveda.flexiq.model.QueueStats;
import org.byteveda.flexiq.model.WorkerInfo;

/**
 * Operational endpoints: circuit breakers, event types, the KEDA scaler payload,
 * readiness, and the Prometheus exposition. The scaler and Prometheus outputs are
 * computed purely from stats + workers.
 */
public final class OpsHandlers {
    private static final long DEFAULT_TARGET_QUEUE_DEPTH = 10;

    private final FlexiQ queue;

    /**
     * Handlers reading one queue's operational state.
     *
     * @param queue what the routes below read from
     */
    public OpsHandlers(FlexiQ queue) {
        this.queue = queue;
    }

    /**
     * Every task's circuit-breaker state.
     *
     * @return one row per task with a breaker
     */
    public Object circuitBreakers() {
        return queue.listCircuitBreakers().stream()
                .map(Contract::circuitBreaker)
                .collect(Collectors.toList());
    }

    /**
     * Every event wire name a webhook may subscribe to.
     *
     * @return the names, for the webhook editor's picker
     */
    public Object eventTypes() {
        List<String> types = new ArrayList<>();
        for (EventName name : EventName.values()) {
            types.add(name.wireName());
        }
        return types;
    }

    /**
     * The KEDA external-scaler payload: depth, target, and live capacity.
     *
     * @param query {@code queue} to report on one queue rather than all, and
     *     {@code target} for the depth an autoscaler aims at per replica
     * @return the payload, including a per-queue breakdown when no queue was named
     */
    public Object scaler(Map<String, String> query) {
        String queueName = query.get("queue");
        long target = Http.longParam(query, "target", DEFAULT_TARGET_QUEUE_DEPTH);
        QueueStats stats = queueName == null ? queue.stats() : queue.statsByQueue(queueName);
        List<WorkerInfo> workers = queue.listWorkers();
        long capacity = workers.stream().mapToLong(w -> w.threads).sum();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("metric_name", queueName == null ? "flexiq_queue_depth" : "flexiq_queue_depth_" + queueName);
        out.put("metric_value", stats.pending);
        out.put("target_queue_depth", target);
        out.put("is_active", stats.pending > 0 || stats.running > 0);
        out.put("live_workers", workers.size());
        out.put("total_capacity", capacity);
        if (queueName == null) {
            Map<String, Object> perQueue = new LinkedHashMap<>();
            queue.statsAllQueues().forEach((name, s) -> perQueue.put(name, s.pending));
            out.put("per_queue", perQueue);
        }
        return out;
    }

    /**
     * Per-resource status aggregated across workers, derived from each worker's
     * advertised {@code resources} + reported {@code resourceHealth}. A resource
     * a worker advertises but never reports on is {@code not_initialized}; when
     * workers disagree, the worst health wins.
     *
     * @return one row per resource any live worker knows about
     */
    public Object resources() {
        return Health.resourceStatus(queue).stream()
                .map(ResourceStatusEntry::toMap)
                .toList();
    }

    /**
     * Real dependency checks — the endpoint used to answer {@code ready} unconditionally.
     *
     * @return the readiness body, {@code degraded} rather than thrown when a
     *     dependency fails
     */
    public Object readiness() {
        return Health.readiness(queue).toMap();
    }

    /**
     * Prometheus text exposition of the queue's job counts and worker count.
     *
     * @return the exposition body, ready to serve as {@code text/plain}
     */
    public String prometheus() {
        QueueStats stats = queue.stats();
        StringBuilder sb = new StringBuilder();
        gauge(sb, "flexiq_jobs_pending", "Jobs waiting to run", stats.pending);
        gauge(sb, "flexiq_jobs_running", "Jobs currently running", stats.running);
        gauge(sb, "flexiq_jobs_completed", "Jobs completed", stats.completed);
        gauge(sb, "flexiq_jobs_failed", "Jobs failed", stats.failed);
        gauge(sb, "flexiq_jobs_dead", "Jobs dead-lettered", stats.dead);
        gauge(sb, "flexiq_jobs_cancelled", "Jobs cancelled", stats.cancelled);
        gauge(sb, "flexiq_workers", "Registered workers", queue.listWorkers().size());
        return sb.toString();
    }

    private static void gauge(StringBuilder sb, String name, String help, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append(' ').append(value).append('\n');
    }
}
