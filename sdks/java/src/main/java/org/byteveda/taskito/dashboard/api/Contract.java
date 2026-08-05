package org.byteveda.taskito.dashboard.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.byteveda.taskito.model.CircuitBreakerState;
import org.byteveda.taskito.model.DagEdge;
import org.byteveda.taskito.model.DeadJob;
import org.byteveda.taskito.model.EffectiveRetention;
import org.byteveda.taskito.model.Job;
import org.byteveda.taskito.model.JobDag;
import org.byteveda.taskito.model.QueueStats;
import org.byteveda.taskito.model.ReplayEntry;
import org.byteveda.taskito.model.RetentionPreview;
import org.byteveda.taskito.model.TaskLog;
import org.byteveda.taskito.model.WorkerInfo;
import org.byteveda.taskito.model.WorkflowRunInfo;
import org.byteveda.taskito.webhooks.Delivery;
import org.byteveda.taskito.webhooks.Webhook;
import org.byteveda.taskito.workflows.NodeSnapshot;
import org.jspecify.annotations.Nullable;

/**
 * Maps the SDK's camelCase types to the snake_case JSON contract the React SPA
 * expects. Timestamps are Unix milliseconds throughout (never scaled).
 */
final class Contract {
    private Contract() {}

    static Map<String, Object> stats(QueueStats s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("pending", s.pending);
        m.put("running", s.running);
        m.put("completed", s.completed);
        m.put("failed", s.failed);
        m.put("dead", s.dead);
        m.put("cancelled", s.cancelled);
        return m;
    }

    static Map<String, Object> job(Job j) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", j.id);
        m.put("task_name", j.taskName);
        m.put("queue", j.queue);
        m.put("status", j.status.wire());
        m.put("priority", j.priority);
        m.put("progress", j.progress);
        m.put("retry_count", j.retryCount);
        m.put("max_retries", j.maxRetries);
        m.put("created_at", j.createdAt);
        m.put("scheduled_at", j.scheduledAt);
        m.put("started_at", j.startedAt);
        m.put("completed_at", j.completedAt);
        m.put("timeout_ms", j.timeoutMs);
        m.put("error", j.error);
        m.put("unique_key", j.uniqueKey);
        m.put("namespace", j.namespace);
        m.put("notes", j.notes);
        return m;
    }

    static Map<String, Object> worker(WorkerInfo w) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("worker_id", w.workerId);
        m.put("queues", w.queues);
        m.put("status", w.status);
        m.put("last_heartbeat", w.lastHeartbeat);
        m.put("registered_at", w.startedAt == null ? w.lastHeartbeat : w.startedAt);
        m.put("hostname", w.hostname);
        m.put("pid", w.pid);
        m.put("pool_type", w.poolType);
        m.put("threads", w.threads);
        m.put("tags", w.tags);
        return m;
    }

    static Map<String, Object> circuitBreaker(CircuitBreakerState c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("task_name", c.taskName);
        m.put("state", c.state);
        m.put("failure_count", c.failureCount);
        m.put("threshold", c.threshold);
        m.put("window_ms", c.windowMs);
        m.put("cooldown_ms", c.cooldownMs);
        m.put("opened_at", c.openedAt);
        m.put("last_failure_at", c.lastFailureAt);
        m.put("half_open_max_probes", c.halfOpenMaxProbes);
        m.put("half_open_success_rate", c.halfOpenSuccessRate);
        return m;
    }

    static Map<String, Object> dead(DeadJob d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", d.id);
        m.put("original_job_id", d.originalJobId);
        m.put("task_name", d.taskName);
        m.put("queue", d.queue);
        m.put("error", d.error);
        m.put("retry_count", d.retryCount);
        m.put("failed_at", d.failedAt);
        m.put("metadata", d.metadata);
        m.put("dlq_retry_count", d.dlqRetryCount);
        return m;
    }

    static Map<String, Object> taskLog(TaskLog l) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", l.id);
        m.put("job_id", l.jobId);
        m.put("task_name", l.taskName);
        m.put("level", l.level);
        m.put("message", l.message);
        m.put("extra", l.extra);
        m.put("logged_at", l.loggedAt);
        return m;
    }

    static Map<String, Object> replayEntry(ReplayEntry r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("replay_job_id", r.replayJobId);
        m.put("replayed_at", r.replayedAt);
        m.put("original_error", r.originalError);
        m.put("replay_error", r.replayError);
        return m;
    }

    static Map<String, Object> jobDag(JobDag dag) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodes", dag.nodes.stream().map(Contract::dagNode).collect(Collectors.toList()));
        m.put("edges", dag.edges.stream().map(Contract::dagEdge).collect(Collectors.toList()));
        return m;
    }

    private static Map<String, Object> dagNode(Job j) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", j.id);
        m.put("task_name", j.taskName);
        m.put("status", j.status.wire());
        return m;
    }

    private static Map<String, Object> dagEdge(DagEdge e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("from", e.from);
        m.put("to", e.to);
        return m;
    }

    static Map<String, Object> workflowRun(WorkflowRunInfo r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.id);
        m.put("definition_id", r.definitionId);
        m.put("state", r.state);
        m.put("params", r.params);
        m.put("started_at", r.startedAt);
        m.put("completed_at", r.completedAt);
        m.put("error", r.error);
        m.put("parent_run_id", r.parentRunId);
        m.put("parent_node_name", r.parentNodeName);
        m.put("created_at", r.createdAt);
        return m;
    }

    /**
     * Map a webhook to the SPA contract. The raw secret is NEVER emitted — only a
     * {@code has_secret} flag — and every header VALUE is masked, since headers
     * routinely carry outbound credentials. Use {@link #webhookWithSecret} for the
     * create/rotate responses that surface the secret exactly once.
     */
    static Map<String, Object> webhook(Webhook w) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", w.id);
        m.put("url", w.url);
        m.put("events", w.events);
        m.put("task_filter", w.taskFilter);
        m.put("headers", maskHeaderValues(w.headers));
        m.put("has_secret", w.secret != null);
        m.put("max_retries", w.maxRetries);
        m.put("timeout_seconds", w.timeoutMs / 1000.0);
        m.put("retry_backoff", w.retryBackoff);
        m.put("enabled", w.enabled);
        m.put("description", w.description);
        m.put("created_at", w.createdAt);
        m.put("updated_at", w.updatedAt);
        return m;
    }

    /** As {@link #webhook} but with the raw secret surfaced once (create/rotate only). */
    static Map<String, Object> webhookWithSecret(Webhook w) {
        Map<String, Object> m = webhook(w);
        m.put("secret", w.secret);
        return m;
    }

    static Map<String, Object> delivery(Delivery d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", d.id());
        m.put("subscription_id", d.subscriptionId());
        m.put("event", d.event());
        m.put("task_name", d.taskName());
        m.put("job_id", d.jobId());
        m.put("status", d.status());
        m.put("attempts", d.attempts());
        m.put("response_code", d.responseCode());
        m.put("response_body", d.responseBody());
        m.put("latency_ms", d.latencyMs());
        m.put("error", d.error());
        m.put("created_at", d.createdAt());
        m.put("completed_at", d.completedAt());
        return m;
    }

    /** Replace every header value with a mask so outbound credentials aren't exposed. */
    private static Map<String, Object> maskHeaderValues(Map<String, String> headers) {
        Map<String, Object> masked = new LinkedHashMap<>();
        headers.forEach((name, value) -> masked.put(name, "***"));
        return masked;
    }

    /**
     * The published retention policy, or the unreported state when {@code r} is
     * null. {@code reported} is distinct from {@code enabled}: no worker has
     * swept yet, which is not the same as retention being switched off.
     */
    static Map<String, Object> retention(@Nullable EffectiveRetention r) {
        Map<String, Object> windows = new LinkedHashMap<>();
        windows.put("task_logs_ttl_ms", r == null ? null : r.windows.taskLogsMs);
        windows.put("archived_jobs_ttl_ms", r == null ? null : r.windows.archivedJobsMs);
        windows.put("job_errors_ttl_ms", r == null ? null : r.windows.jobErrorsMs);
        windows.put("task_metrics_ttl_ms", r == null ? null : r.windows.taskMetricsMs);
        windows.put("dead_letter_ttl_ms", r == null ? null : r.windows.deadLetterMs);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("reported", r != null);
        m.put("enabled", r != null && r.enabled);
        m.put("defaulted", r != null && r.defaulted);
        m.put("namespace", r == null ? null : r.namespace);
        m.put("reported_at", r == null ? null : r.reportedAt);
        m.put("windows", windows);
        return m;
    }

    /** The dry-run preview: per-table counts a purge would remove, plus the
     *  windows they were computed against. Always present — computed in-process. */
    static Map<String, Object> retentionDryRun(RetentionPreview p) {
        Map<String, Object> windows = new LinkedHashMap<>();
        windows.put("task_logs_ttl_ms", p.windows.taskLogsMs);
        windows.put("archived_jobs_ttl_ms", p.windows.archivedJobsMs);
        windows.put("job_errors_ttl_ms", p.windows.jobErrorsMs);
        windows.put("task_metrics_ttl_ms", p.windows.taskMetricsMs);
        windows.put("dead_letter_ttl_ms", p.windows.deadLetterMs);

        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("task_logs", p.counts.taskLogs);
        counts.put("archived_jobs", p.counts.archivedJobs);
        counts.put("job_errors", p.counts.jobErrors);
        counts.put("task_metrics", p.counts.taskMetrics);
        counts.put("dead_letter", p.counts.deadLetter);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", p.enabled);
        m.put("defaulted", p.defaulted);
        m.put("namespace", p.namespace);
        m.put("reference_time", p.referenceTime);
        m.put("windows", windows);
        m.put("counts", counts);
        m.put("total", p.total);
        return m;
    }

    static Map<String, Object> workflowNode(NodeSnapshot n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("node_name", n.nodeName);
        m.put("status", n.status);
        m.put("job_id", n.jobId);
        m.put("result_hash", n.resultHash);
        m.put("fan_out_count", n.fanOutCount);
        m.put("started_at", n.startedAt);
        m.put("completed_at", n.completedAt);
        m.put("error", n.error);
        m.put("compensation_job_id", n.compensationJobId);
        m.put("compensation_started_at", n.compensationStartedAt);
        m.put("compensation_completed_at", n.compensationCompletedAt);
        m.put("compensation_error", n.compensationError);
        return m;
    }
}
