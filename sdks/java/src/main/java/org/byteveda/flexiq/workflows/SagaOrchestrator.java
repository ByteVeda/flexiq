package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.byteveda.flexiq.errors.WorkflowException;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.NodeCompensationEvent;
import org.byteveda.flexiq.events.WorkflowEvent;
import org.byteveda.flexiq.spi.QueueBackend;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.jspecify.annotations.Nullable;

/**
 * Rolls back a failed workflow run. When a run with compensable steps fails, the
 * tracker hands it here: completed steps are compensated in reverse-dependency
 * order — each wave of independent rollbacks runs in parallel, the next wave only
 * after the previous drains. A compensation job is enqueued with the step's
 * forward result as its payload, an idempotency key (so a tracker restart is a
 * no-op), and {@code compensation:true} metadata (so it never advances the
 * forward run). The run ends {@code compensated}, or {@code compensation_failed}
 * if a rollback itself fails.
 */
final class SagaOrchestrator {
    private final QueueBackend backend;
    private final WorkflowTracker tracker;
    private final ObjectMapper json;
    private final ConcurrentMap<String, SagaRun> runs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String[]> compensationJobs = new ConcurrentHashMap<>();

    SagaOrchestrator(QueueBackend backend, WorkflowTracker tracker, ObjectMapper json) {
        this.backend = backend;
        this.tracker = tracker;
        this.json = json;
    }

    boolean isCompensationJob(String jobId) {
        return compensationJobs.containsKey(jobId);
    }

    /** Begin compensating a failed run; returns false (caller finalizes normally) if nothing to roll back. */
    boolean startCompensation(String runId, RunPlan plan, Map<String, NodeSnapshot> statuses) {
        Map<String, CompTarget> targets = new HashMap<>();
        for (PlanNode node : plan.allNodes()) {
            NodeSnapshot snap = statuses.get(node.name);
            if (node.compensate == null || snap == null || !isCompensable(snap.status)) {
                continue;
            }
            if (snap.jobId == null) {
                throw new WorkflowException(
                        "completed workflow node '" + node.name + "' has no forward job to compensate from");
            }
            byte[] forward = backend.getResult(snap.jobId)
                    .orElseThrow(() ->
                            new WorkflowException("missing forward result for workflow node '" + node.name + "'"));
            targets.put(
                    node.name,
                    new CompTarget(
                            node.compensate,
                            forward,
                            node.queueOrDefault(),
                            node.retries(),
                            node.timeout(),
                            node.priorityOrDefault()));
        }
        if (targets.isEmpty()) {
            return false;
        }
        SagaRun run = new SagaRun(reverseTopoWaves(plan, targets.keySet()), targets);
        // Concurrent terminal outcomes can both reach here for the same run; only the
        // first owns it, so the run is marked compensating and dispatched exactly once.
        if (runs.putIfAbsent(runId, run) != null) {
            return true;
        }
        synchronized (run) {
            backend.setWorkflowRunCompensating(runId);
            tracker.emit(new WorkflowEvent(EventName.WORKFLOW_COMPENSATING, runId, null, null));
            dispatchOrFail(runId, run);
        }
        return true;
    }

    /** Record a compensation job's outcome and advance (or finalize) the saga. */
    void onCompensationCompleted(String jobId, boolean succeeded, @Nullable String error) {
        String[] key = compensationJobs.remove(jobId);
        if (key == null) {
            return;
        }
        String runId = key[0];
        String node = key[1];
        SagaRun run = runs.get(runId);
        if (run == null) {
            return;
        }
        // Serialize wave advancement: two outcomes in one wave must not both observe an
        // empty in-flight set and double-dispatch (or skip) the next wave.
        synchronized (run) {
            if (succeeded) {
                backend.setWorkflowNodeCompensated(runId, node, now());
                tracker.emit(new NodeCompensationEvent(EventName.WORKFLOW_NODE_COMPENSATED, runId, node, null));
            } else {
                backend.setWorkflowNodeCompensationFailed(runId, node, error, now());
                tracker.emit(
                        new NodeCompensationEvent(EventName.WORKFLOW_NODE_COMPENSATION_FAILED, runId, node, error));
                run.anyFailed = true;
            }
            run.inflight.remove(node);
            if (run.inflight.isEmpty()) {
                if (run.anyFailed) {
                    finalizeSaga(runId, run); // first failure stops further rollback
                } else {
                    dispatchOrFail(runId, run);
                }
            }
        }
    }

    /** Dispatch the next wave, finalizing the run instead of stranding it in COMPENSATING if enqueue fails. */
    private void dispatchOrFail(String runId, SagaRun run) {
        try {
            dispatchNextWave(runId, run);
        } catch (RuntimeException e) {
            run.anyFailed = true;
            finalizeSaga(runId, run);
            throw e;
        }
    }

    /** Enqueue the next wave's compensation jobs. The caller must hold {@code run}'s monitor. */
    private void dispatchNextWave(String runId, SagaRun run) {
        if (run.waveIndex >= run.waves.size()) {
            finalizeSaga(runId, run);
            return;
        }
        List<String> wave = run.waves.get(run.waveIndex++);
        run.inflight.addAll(wave);
        for (String node : wave) {
            // The runtime job id (the saga's routing key) is the value enqueue returns,
            // not the idempotency key we pass in — so register routing once we have it.
            CompTarget target = Objects.requireNonNull(
                    run.targets.get(node), () -> "no compensation target for node '" + node + "'");
            String compJobId = enqueueCompensation(runId, node, target);
            compensationJobs.put(compJobId, new String[] {runId, node});
            backend.setWorkflowNodeCompensationJob(runId, node, compJobId, now());
            tracker.emit(new NodeCompensationEvent(EventName.WORKFLOW_NODE_COMPENSATING, runId, node, null));
        }
    }

    private String enqueueCompensation(String runId, String node, CompTarget target) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("compensation", true);
        metadata.put("workflow_run_id", runId);
        metadata.put("workflow_node_name", node);
        try {
            EnqueueOptions options = EnqueueOptions.builder()
                    .jobId("compensation:" + runId + ":" + node)
                    .metadata(json.writeValueAsString(metadata))
                    .queue(target.queue)
                    .maxRetries(target.retries)
                    .timeoutMs(target.timeout)
                    .priority(target.priority)
                    .build();
            return backend.enqueue(target.task, target.payload, json.writeValueAsString(options));
        } catch (Exception e) {
            throw new WorkflowException("failed to enqueue compensation for node '" + node + "'", e);
        }
    }

    private void finalizeSaga(String runId, SagaRun run) {
        runs.remove(runId);
        compensationJobs.values().removeIf(key -> key[0].equals(runId));
        if (run.anyFailed) {
            backend.setWorkflowRunCompensationFailed(runId, now(), "saga compensation failed");
            tracker.emit(
                    new WorkflowEvent(EventName.WORKFLOW_COMPENSATION_FAILED, runId, null, "saga compensation failed"));
        } else {
            backend.setWorkflowRunCompensated(runId, now());
            tracker.emit(new WorkflowEvent(EventName.WORKFLOW_COMPENSATED, runId, null, null));
        }
        tracker.forget(runId);
    }

    /** Compensable nodes grouped by topo depth, deepest first (roll back dependents before dependencies). */
    private List<List<String>> reverseTopoWaves(RunPlan plan, Set<String> compensable) {
        Map<String, Integer> depth = new HashMap<>();
        for (String node : compensable) {
            computeDepth(plan, node, depth);
        }
        Map<Integer, List<String>> byDepth = new TreeMap<>(Collections.reverseOrder());
        for (String node : compensable) {
            byDepth.computeIfAbsent(depth.get(node), key -> new ArrayList<>()).add(node);
        }
        return new ArrayList<>(byDepth.values());
    }

    private int computeDepth(RunPlan plan, String node, Map<String, Integer> memo) {
        Integer cached = memo.get(node);
        if (cached != null) {
            return cached;
        }
        PlanNode planNode = plan.node(node);
        int depth = 0;
        if (planNode != null) {
            for (String predecessor : planNode.predecessors) {
                depth = Math.max(depth, computeDepth(plan, predecessor, memo) + 1);
            }
        }
        memo.put(node, depth);
        return depth;
    }

    private static boolean isCompensable(NodeStatus status) {
        // Only nodes that actually executed their forward task this run. A CACHE_HIT
        // skipped the task, so its side effects were never performed here.
        return status == NodeStatus.COMPLETED;
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    /** What to enqueue to roll back one node. */
    private static final class CompTarget {
        final String task;
        final byte[] payload;
        final String queue;
        final int retries;
        final long timeout;
        final int priority;

        CompTarget(String task, byte[] payload, String queue, int retries, long timeout, int priority) {
            this.task = task;
            this.payload = payload;
            this.queue = queue;
            this.retries = retries;
            this.timeout = timeout;
            this.priority = priority;
        }
    }

    /** Per-run compensation progress. */
    private static final class SagaRun {
        final List<List<String>> waves;
        final Map<String, CompTarget> targets;
        final Set<String> inflight = ConcurrentHashMap.newKeySet();
        int waveIndex;
        volatile boolean anyFailed;

        SagaRun(List<List<String>> waves, Map<String, CompTarget> targets) {
            this.waves = waves;
            this.targets = targets;
        }
    }
}
