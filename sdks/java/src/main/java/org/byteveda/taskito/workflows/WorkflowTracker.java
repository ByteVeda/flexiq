package org.byteveda.taskito.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.byteveda.taskito.errors.SerializationException;
import org.byteveda.taskito.errors.WorkflowException;
import org.byteveda.taskito.events.Emitter;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.events.GateEvent;
import org.byteveda.taskito.events.OutcomeEvent;
import org.byteveda.taskito.events.TaskitoEvent;
import org.byteveda.taskito.events.WorkflowEvent;
import org.byteveda.taskito.logging.TaskitoLogger;
import org.byteveda.taskito.serialization.Serializer;
import org.byteveda.taskito.spi.QueueBackend;
import org.jspecify.annotations.Nullable;

/**
 * Advances workflow node and run state from worker outcomes. Attach via
 * {@code Worker.Builder.trackWorkflows()}.
 *
 * <p>On each terminal job outcome it records the node result, then drives the
 * run forward. A static node finalizes the run when all nodes settle. A fan-out
 * producer's result is split into per-item child jobs; a settled fan-out is
 * collected into one fan-in job. An approval <em>gate</em> parks
 * ({@code WAITING_APPROVAL}) until {@code Worker.approveGate}/{@code rejectGate}
 * (or its timeout) resolves it; its successors then run. Deferred non-fan nodes
 * (gates and steps downstream of a gate or fan-in) carry a node row but no job
 * at submit — the tracker creates their job once predecessors settle, taking
 * their payload from a {@link Workflow} registered with
 * {@code trackWorkflows(workflow)}.
 *
 * <p>Each run's DAG (the plan) is loaded lazily from storage and cached until the
 * run reaches a terminal state.
 */
public final class WorkflowTracker {
    private static final TaskitoLogger LOG = TaskitoLogger.create("workflows");
    private static final int FAIL_RECORD_MAX_ATTEMPTS = 5;
    private static final long FAIL_RECORD_RETRY_MS = 1_000;

    private final QueueBackend backend;
    private final Serializer serializer;
    private final ObjectMapper json = new ObjectMapper();
    private final ConcurrentMap<String, RunPlan> plans = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> runNames = new ConcurrentHashMap<>();
    private final ScheduledExecutorService gateScheduler =
            Executors.newSingleThreadScheduledExecutor(WorkflowTracker::daemon);

    /** Deferred-node payloads from registered workflows: {@code wfName -> node -> serialized payload}. */
    private final ConcurrentMap<String, Map<String, byte[]>> deferredPayloads = new ConcurrentHashMap<>();
    /** Sub-workflow child runs' deferred-node payloads, keyed by run id so same-named runs don't collide. */
    private final ConcurrentMap<String, Map<String, byte[]>> childRunPayloads = new ConcurrentHashMap<>();
    /** Callable conditions from registered workflows: {@code wfName -> node -> predicate}. */
    private final ConcurrentMap<String, Map<String, Condition>> callableConditions = new ConcurrentHashMap<>();
    /** Live gate timeout timers, so a manual resolution can cancel a pending auto-resolution. */
    private final ConcurrentMap<GateKey, ScheduledFuture<?>> gateTimers = new ConcurrentHashMap<>();
    /** Gate keys already resolved, so a timer and a manual call never both fire. */
    private final Set<GateKey> resolvedGates = ConcurrentHashMap.newKeySet();
    /** Deferred nodes already promoted (job created or gate parked), to dedupe concurrent outcomes. */
    private final Set<GateKey> promotedNodes = ConcurrentHashMap.newKeySet();
    /** Sub-workflow child run → its parent {@code [runId, nodeName]}, so a finalized child resolves its parent node. */
    private final ConcurrentMap<String, String[]> childToParent = new ConcurrentHashMap<>();
    /** Rolls back failed runs that declared compensators. */
    private final SagaOrchestrator saga;
    /** Incremental cache: {@code cacheKey -> expiry millis}, so a re-run of a cacheable step is a cache hit. */
    private final ConcurrentMap<String, Long> resultCache = new ConcurrentHashMap<>();

    /** The owning worker's emitter (bound at worker start); null until then. */
    private volatile @Nullable Emitter emitter;

    public WorkflowTracker(QueueBackend backend, Serializer serializer) {
        this.backend = backend;
        this.serializer = serializer;
        this.saga = new SagaOrchestrator(backend, this, json);
    }

    /**
     * Register a workflow so the tracker can supply its deferred nodes' payloads
     * at runtime. Required for any workflow with a gate (or other deferred node)
     * whose downstream steps run a task.
     */
    public void register(Workflow workflow) {
        Map<String, byte[]> byNode = new HashMap<>();
        Map<String, Condition> conditions = new HashMap<>();
        for (Step step : workflow.steps()) {
            if (step.payload != null) {
                byNode.put(step.name, serializer.serializeCall(step.payload));
            }
            if (step.callableCondition != null) {
                conditions.put(step.name, step.callableCondition);
            }
        }
        deferredPayloads.put(workflow.name(), byNode);
        callableConditions.put(workflow.name(), conditions);
    }

    /** Bind the emitter workflow lifecycle events dispatch through (wired by the worker at start). */
    public void bindEmitter(Emitter emitter) {
        this.emitter = emitter;
    }

    /** Emit a workflow event through the bound emitter; a no-op until one is bound. */
    void emit(TaskitoEvent event) {
        Emitter bound = emitter;
        if (bound != null) {
            bound.emit(event);
        }
    }

    /** Route a successful job outcome. */
    public void onSuccess(OutcomeEvent event) {
        onOutcome(event.jobId, true, null);
    }

    /** Route a dead-lettered (terminally failed) job outcome. */
    public void onDead(OutcomeEvent event) {
        onOutcome(event.jobId, false, event.error);
    }

    private void onOutcome(String jobId, boolean succeeded, @Nullable String error) {
        // Compensation jobs carry compensation:true metadata, so they never map to
        // a forward node — route them to the saga instead.
        if (saga.isCompensationJob(jobId)) {
            saga.onCompensationCompleted(jobId, succeeded, error);
            return;
        }
        NodeRef ref = backend.workflowNodeForJobJson(jobId)
                .map(raw -> decode(raw, NodeRef.class))
                .orElse(null);
        if (ref == null) {
            return; // not a workflow job
        }
        // Record the node outcome; the tracker (not the core) drives cascade + finalize.
        backend.markWorkflowNodeResult(jobId, succeeded, error, true);
        RunPlan plan = plans.computeIfAbsent(ref.runId, this::loadPlan);

        // Cache a cacheable step's success so a later run of the same workflow can skip it.
        if (succeeded && plan != null) {
            PlanNode settled = plan.node(ref.nodeName);
            if (settled != null && settled.cache != null) {
                cacheResult(ref.runId, settled);
            }
        }

        if (ref.nodeName.indexOf('[') >= 0) {
            handleFanOutChild(ref.runId, ref.nodeName, plan);
            return;
        }
        if (plan == null) {
            // No plan to drive (e.g. an untracked run) — fall back to fail-fast.
            if (!succeeded) {
                backend.cascadeSkipPending(ref.runId);
            }
            finalizeIfTerminal(ref.runId);
            return;
        }
        if (succeeded) {
            expandFanOutIfAny(ref.runId, ref.nodeName, jobId, plan);
        }
        // Evaluate every successor's condition — running on-failure recovery nodes
        // and skip-cascading the rest. Replaces a blanket fail-fast cascade.
        promoteSuccessors(ref.runId, ref.nodeName, plan);
        finalizeIfTerminal(ref.runId);
    }

    /**
     * If the just-completed node feeds fan-outs, split its result list into child
     * jobs. Failures are scoped per branch: an unusable producer result fails
     * every branch, but one branch's expansion error never touches a sibling
     * whose children are already running.
     */
    private void expandFanOutIfAny(String runId, String node, String jobId, RunPlan plan) {
        List<PlanNode> fanOuts = plan.successorsMatching(node, candidate -> candidate.fanOut != null);
        if (fanOuts.isEmpty()) {
            return;
        }
        List<?> items;
        try {
            items = serializer.deserialize(fetchResult(jobId), List.class);
        } catch (RuntimeException e) {
            LOG.warn("reading fan-out producer result of node '" + node + "' in run " + runId + " failed", e);
            for (PlanNode fanOut : fanOuts) {
                failFanOut(runId, fanOut, plan, e);
            }
            return;
        }
        for (PlanNode fanOut : fanOuts) {
            try {
                expandFanOut(runId, fanOut, items, plan);
            } catch (RuntimeException e) {
                LOG.warn("expanding fan-out node '" + fanOut.name + "' in run " + runId + " failed", e);
                failFanOut(runId, fanOut, plan, e);
            }
        }
    }

    /** Split the producer's items into one child job per element of {@code fanOut}. */
    private void expandFanOut(String runId, PlanNode fanOut, List<?> items, RunPlan plan) {
        String[] names = new String[items.size()];
        byte[][] payloads = new byte[items.size()][];
        for (int i = 0; i < items.size(); i++) {
            names[i] = fanOut.name + "[" + i + "]";
            payloads[i] = serializer.serializeCall(items.get(i));
        }
        backend.expandFanOut(
                runId,
                fanOut.name,
                names,
                payloads,
                fanOut.taskName,
                fanOut.queueOrDefault(),
                fanOut.retries(),
                fanOut.timeout(),
                fanOut.priorityOrDefault());
        // An empty producer list yields no child jobs, so no child outcome will
        // ever drive the fan-in — collect it now with an empty result list.
        if (items.isEmpty()) {
            collectFanIns(runId, fanOut.name, List.of(), plan);
        }
    }

    /** When every fan-out child has settled, collect their results into the fan-in job(s). */
    private void handleFanOutChild(String runId, String childName, RunPlan plan) {
        String parent = childName.substring(0, childName.indexOf('['));
        FanOutCompletion done = backend.checkFanOutCompletionJson(runId, parent)
                .map(raw -> decode(raw, FanOutCompletion.class))
                .orElse(null);
        if (done == null) {
            return; // siblings still running, or already handled by a concurrent outcome
        }
        if (!done.succeeded) {
            // The fan-out parent is now Failed; evaluate its successors' conditions
            // (on_failure/always run, on_success skip-cascade) like any other node.
            if (plan != null) {
                promoteSuccessors(runId, parent, plan);
            } else {
                backend.cascadeSkipPending(runId);
            }
            finalizeIfTerminal(runId);
            return;
        }
        List<Object> results = new ArrayList<>(done.childJobIds.size());
        for (String childJobId : done.childJobIds) {
            results.add(serializer.deserialize(fetchResult(childJobId), Object.class));
        }
        collectFanIns(runId, parent, results, plan);
    }

    /** Create a fan-in job for every fan-in successor of {@code parent}; promote others; finalize if none. */
    private void collectFanIns(String runId, String parent, List<Object> results, RunPlan plan) {
        List<PlanNode> fanIns =
                plan == null ? List.of() : plan.successorsMatching(parent, candidate -> candidate.fanIn != null);
        if (fanIns.isEmpty()) {
            if (plan != null) {
                promoteSuccessors(runId, parent, plan);
            }
            finalizeIfTerminal(runId);
            return;
        }
        byte[] payload = serializer.serializeCall(results);
        for (PlanNode fanIn : fanIns) {
            backend.createDeferredJob(
                    runId,
                    fanIn.name,
                    payload,
                    fanIn.taskName,
                    fanIn.queueOrDefault(),
                    fanIn.retries(),
                    fanIn.timeout(),
                    fanIn.priorityOrDefault());
        }
    }

    /**
     * Evaluate every successor of {@code settledNode} whose predecessors have all
     * settled: run it (its condition holds) or skip-and-cascade it. A satisfied
     * gate parks; a satisfied deferred node's job is created; a satisfied static
     * node is left for the scheduler; an unsatisfied node is skipped and the skip
     * cascades to its own successors.
     */
    private void promoteSuccessors(String runId, String settledNode, RunPlan plan) {
        Map<String, NodeSnapshot> statuses = statusMap(runId);
        for (PlanNode succ : plan.successorsMatching(settledNode, candidate -> true)) {
            promoteOrSkip(runId, succ, statuses, plan);
        }
    }

    private void promoteOrSkip(String runId, PlanNode node, Map<String, NodeSnapshot> statuses, RunPlan plan) {
        NodeSnapshot snap = statuses.get(node.name);
        if (snap == null || snap.status != NodeStatus.PENDING) {
            return; // already running, parked, promoted, or terminal
        }
        if (!allTerminal(node.predecessors, statuses)) {
            return; // a predecessor is still in flight
        }
        GateKey promotionKey = new GateKey(runId, node.name);
        if (!promotedNodes.add(promotionKey)) {
            return; // a concurrent outcome already decided this node
        }
        // Roll back the dedupe marker if promotion fails, else the node wedges PENDING.
        try {
            if (!shouldRun(runId, node, statuses)) {
                backend.skipWorkflowNode(runId, node.name);
                promoteSuccessors(runId, node.name, plan); // cascade the skip downstream
                return;
            }
            if (node.fanOut != null || node.fanIn != null) {
                return; // fan nodes are driven by expandFanOut / fan-out completion
            }
            if (snap.jobId != null) {
                return; // static node — the scheduler runs it once its deps complete
            }
            if (node.gate != null) {
                enterGate(runId, node);
                return;
            }
            if (node.subWorkflow != null) {
                submitSubWorkflow(runId, node);
                return;
            }
            if (node.cache != null && reuseFromCache(runId, node, plan)) {
                return; // cache hit — skipped re-running this step
            }
            createDeferredJobFor(runId, node);
        } catch (RuntimeException e) {
            // A failed promotion must never leave the node wedged PENDING with no
            // trace — outcome listeners swallow throws, so rethrowing would
            // silently stall the run.
            LOG.warn("promoting workflow node '" + node.name + "' in run " + runId + " failed", e);
            failNodeAndCascade(runId, node.name, promotionKey, plan, e);
        }
    }

    /** Mark a node Failed so the run still settles; on storage failure, allow a later retry. */
    private void failNodeAndCascade(
            String runId, String nodeName, GateKey promotionKey, RunPlan plan, RuntimeException cause) {
        try {
            backend.failWorkflowNode(runId, nodeName, describe(cause));
            promoteSuccessors(runId, nodeName, plan);
            finalizeIfTerminal(runId);
        } catch (RuntimeException e) {
            // Even the failure record failed (e.g. storage down). Roll the dedupe
            // marker back so the next outcome can retry the promotion.
            promotedNodes.remove(promotionKey);
            LOG.error("failed to record failure of workflow node '" + nodeName + "' in run " + runId, e);
        }
    }

    /**
     * Fail one fan-out node (its expansion failed) and cascade. Unlike deferred
     * nodes, nothing re-drives a fan-out after its producer settles, so a storage
     * failure while recording the node failure is retried on the scheduler
     * (bounded) instead of relying on a later outcome.
     */
    private void failFanOut(String runId, PlanNode fanOut, RunPlan plan, RuntimeException cause) {
        failFanOut(runId, fanOut, plan, cause, 1);
    }

    private void failFanOut(String runId, PlanNode fanOut, RunPlan plan, RuntimeException cause, int attempt) {
        try {
            backend.failWorkflowNode(runId, fanOut.name, describe(cause));
            promoteSuccessors(runId, fanOut.name, plan);
            finalizeIfTerminal(runId);
        } catch (RuntimeException e) {
            promotedNodes.remove(new GateKey(runId, fanOut.name));
            if (attempt >= FAIL_RECORD_MAX_ATTEMPTS) {
                LOG.error(
                        "failed to record failure of fan-out node '" + fanOut.name + "' in run " + runId + " after "
                                + attempt + " attempts; run cannot finalize",
                        e);
                return;
            }
            LOG.warn(
                    "recording failure of fan-out node '" + fanOut.name + "' in run " + runId + " failed (attempt "
                            + attempt + "); retrying",
                    e);
            gateScheduler.schedule(
                    () -> failFanOut(runId, fanOut, plan, cause, attempt + 1),
                    FAIL_RECORD_RETRY_MS,
                    TimeUnit.MILLISECONDS);
        }
    }

    private static String describe(RuntimeException cause) {
        String message = cause.getMessage();
        return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /** If a prior run cached this step's result within its TTL, mark it a cache hit instead of running it. */
    private boolean reuseFromCache(String runId, PlanNode node, RunPlan plan) {
        String key = cacheKey(runId, node.name);
        Long expiry = resultCache.get(key);
        if (expiry == null) {
            return false;
        }
        if (expiry <= System.currentTimeMillis()) {
            resultCache.remove(key, expiry); // drop the stale entry rather than let it linger
            return false;
        }
        backend.setWorkflowNodeCacheHit(runId, node.name);
        promoteSuccessors(runId, node.name, plan); // CacheHit is terminal — advance successors
        return true;
    }

    private void cacheResult(String runId, PlanNode node) {
        CacheMeta meta = decode(node.cache, CacheMeta.class);
        if (meta != null && meta.ttlMs != null) {
            long now = System.currentTimeMillis();
            resultCache.values().removeIf(expiry -> expiry <= now); // sweep expired keys before adding
            resultCache.put(cacheKey(runId, node.name), now + meta.ttlMs);
        }
    }

    /** Stable across runs: workflow name + node + a hash of the node's payload. */
    private String cacheKey(String runId, String nodeName) {
        byte[] payload = deferredPayload(runId, nodeName);
        return workflowName(runId) + ":" + nodeName + ":" + sha256(payload == null ? new byte[0] : payload);
    }

    private static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new WorkflowException("failed to hash payload for the workflow cache", e);
        }
    }

    /** Submit a sub-workflow node's child run and mark the node running until the child finalizes. */
    private void submitSubWorkflow(String runId, PlanNode node) {
        ChildSpec child = decode(node.subWorkflow, ChildSpec.class);
        Set<String> deferred = new HashSet<>(child.deferred);
        List<String> payloadNames = new ArrayList<>();
        List<byte[]> payloadBytes = new ArrayList<>();
        Map<String, byte[]> allPayloads = new HashMap<>();
        child.payloads.forEach((nodeName, base64) -> {
            byte[] bytes = java.util.Base64.getDecoder().decode(base64);
            allPayloads.put(nodeName, bytes);
            if (!deferred.contains(nodeName)) {
                payloadNames.add(nodeName);
                payloadBytes.add(bytes);
            }
        });
        String childRun = backend.submitWorkflow(
                child.name,
                child.version,
                child.stepsJson,
                payloadNames.toArray(new String[0]),
                payloadBytes.toArray(new byte[0][]),
                null,
                null,
                deferred.toArray(new String[0]),
                runId,
                node.name);
        // Register the child's payloads under its run id so the tracker can promote the
        // child's own deferred nodes — run-scoped, so two same-named child runs can't stomp.
        childRunPayloads.put(childRun, allPayloads);
        childToParent.put(childRun, new String[] {runId, node.name});
        backend.setWorkflowNodeRunning(runId, node.name);
        emit(new WorkflowEvent(EventName.WORKFLOW_SUBMITTED, childRun, child.name, null));
    }

    /** Whether {@code node}'s condition holds given its predecessors' outcomes. */
    private boolean shouldRun(String runId, PlanNode node, Map<String, NodeSnapshot> statuses) {
        if ("callable".equals(node.condition)) {
            Condition predicate = callableCondition(runId, node.name);
            if (predicate == null) {
                throw new WorkflowException("callable condition for workflow node '" + node.name
                        + "' is not registered; track the workflow on the worker via trackWorkflows(workflow)");
            }
            return predicate.test(buildContext(runId, statuses));
        }
        if ("on_failure".equals(node.condition)) {
            return anyFailed(node.predecessors, statuses);
        }
        if ("always".equals(node.condition)) {
            return true;
        }
        // null / "on_success": run only when every predecessor completed.
        return allCompleted(node.predecessors, statuses);
    }

    private @Nullable Condition callableCondition(String runId, String nodeName) {
        String wfName = workflowName(runId);
        if (wfName == null) {
            return null;
        }
        return callableConditions.getOrDefault(wfName, Map.of()).get(nodeName);
    }

    /** Build the run snapshot a callable condition sees: completed nodes' results + statuses. */
    private WorkflowContext buildContext(String runId, Map<String, NodeSnapshot> statuses) {
        Map<String, Object> results = new HashMap<>();
        Map<String, NodeStatus> statusByNode = new HashMap<>();
        int succeeded = 0;
        int failed = 0;
        for (NodeSnapshot node : statuses.values()) {
            statusByNode.put(node.nodeName, node.status);
            if (isCompleted(node.status)) {
                succeeded++;
                if (node.jobId != null) {
                    results.put(node.nodeName, serializer.deserialize(fetchResult(node.jobId), Object.class));
                }
            } else if (node.status == NodeStatus.FAILED) {
                failed++;
            }
        }
        return new WorkflowContext(runId, results, statusByNode, succeeded, failed);
    }

    /** Park a gate node for approval, scheduling its timeout auto-resolution if any. */
    private void enterGate(String runId, PlanNode node) {
        backend.setWorkflowNodeWaitingApproval(runId, node.name);
        GateMeta meta = node.gate == null ? null : decode(node.gate, GateMeta.class);
        if (meta != null && meta.timeoutMs != null) {
            GateKey key = new GateKey(runId, node.name);
            gateTimers.put(
                    key,
                    gateScheduler.schedule(
                            () -> onGateTimeout(runId, node.name, meta.onTimeout),
                            meta.timeoutMs,
                            TimeUnit.MILLISECONDS));
        }
        // Emit after the timer is registered: a synchronous listener may resolve
        // the gate immediately, and resolution must find (and cancel) the timer.
        emit(new GateEvent(runId, node.name));
    }

    private void onGateTimeout(String runId, String nodeName, String onTimeout) {
        boolean approve = GateAction.fromWire(onTimeout) == GateAction.APPROVE;
        resolveGate(runId, nodeName, approve, approve ? null : "gate timeout");
    }

    /**
     * Resolve a parked gate: complete it (and run its successors) when
     * {@code approved}, else fail it (and skip its successors). Idempotent — the
     * first of a manual call and a timeout wins.
     */
    public void resolveGate(String runId, String nodeName, boolean approved, @Nullable String error) {
        NodeSnapshot snap = statusMap(runId).get(nodeName);
        if (snap == null || snap.status != NodeStatus.WAITING_APPROVAL) {
            return; // only a parked gate can be resolved (wrong node, not yet parked, or already settled)
        }
        GateKey key = new GateKey(runId, nodeName);
        if (!resolvedGates.add(key)) {
            return; // already resolved by a timer or another caller
        }
        ScheduledFuture<?> timer = gateTimers.remove(key);
        if (timer != null) {
            timer.cancel(false);
        }
        backend.resolveWorkflowGate(runId, nodeName, approved, error);
        RunPlan plan = plans.computeIfAbsent(runId, this::loadPlan);
        if (plan != null) {
            // On reject the gate is Failed, so promoteSuccessors skips its successors.
            promoteSuccessors(runId, nodeName, plan);
        }
        finalizeIfTerminal(runId);
    }

    private void createDeferredJobFor(String runId, PlanNode node) {
        byte[] payload = deferredPayload(runId, node.name);
        if (payload == null) {
            throw new WorkflowException("no payload for deferred workflow node '" + node.name
                    + "'; register the workflow on the worker via trackWorkflows(workflow)");
        }
        backend.createDeferredJob(
                runId,
                node.name,
                payload,
                node.taskName,
                node.queueOrDefault(),
                node.retries(),
                node.timeout(),
                node.priorityOrDefault());
    }

    private byte @Nullable [] deferredPayload(String runId, String nodeName) {
        Map<String, byte[]> childPayloads = childRunPayloads.get(runId);
        if (childPayloads != null) {
            return childPayloads.get(nodeName); // a child run resolves only its run-scoped payloads
        }
        String wfName = workflowName(runId);
        if (wfName == null) {
            return null;
        }
        return deferredPayloads.getOrDefault(wfName, Map.of()).get(nodeName);
    }

    /** The run's workflow-definition name, cached (drives the deferred-payload + condition registries). */
    private @Nullable String workflowName(String runId) {
        String cached = runNames.get(runId);
        if (cached != null) {
            return cached;
        }
        String name = backend.workflowNameForRun(runId).orElse(null);
        if (name != null) {
            runNames.put(runId, name);
        }
        return name;
    }

    private void finalizeIfTerminal(String runId) {
        Map<String, NodeSnapshot> statuses = statusMap(runId);
        if (statuses.isEmpty() || !allNodesTerminal(statuses)) {
            return; // not every node has settled yet
        }
        // A failed top-level run with compensators rolls back BEFORE the core marks
        // it failed, so observers never see a transient Failed before Compensating.
        boolean anyFailed = statuses.values().stream().anyMatch(node -> node.status == NodeStatus.FAILED);
        if (anyFailed && !childToParent.containsKey(runId)) {
            RunPlan plan = plans.computeIfAbsent(runId, this::loadPlan);
            if (plan != null && saga.startCompensation(runId, plan, statuses)) {
                return; // the saga owns the run and will finalize it
            }
        }
        Optional<String> finalState = backend.finalizeRunIfTerminal(runId);
        if (finalState.isEmpty()) {
            return;
        }
        // Resolve the workflow's name BEFORE forget() clears the run-name cache.
        String workflowName = workflowName(runId);
        String[] parent = childToParent.remove(runId);
        forget(runId); // run reached a terminal state — drop its cached state
        emitRunFinalized(runId, workflowName, finalState.get());
        if (parent != null) {
            // This run is a sub-workflow child — resolve its parent node, then advance the parent.
            boolean succeeded = "completed".equals(finalState.get());
            resolveSubWorkflowParent(
                    parent[0],
                    parent[1],
                    succeeded,
                    succeeded ? null : "sub-workflow " + runId + " " + finalState.get());
        }
    }

    /** Emit the run's terminal lifecycle event; unknown states (defensive) emit nothing. */
    private void emitRunFinalized(String runId, @Nullable String workflowName, String state) {
        EventName name =
                switch (state) {
                    case "completed" -> EventName.WORKFLOW_COMPLETED;
                    case "failed" -> EventName.WORKFLOW_FAILED;
                    case "completed_with_failures" -> EventName.WORKFLOW_COMPLETED_WITH_FAILURES;
                    default -> null; // compensation finals are emitted by the saga
                };
        if (name != null) {
            emit(new WorkflowEvent(name, runId, workflowName, null));
        }
    }

    private void resolveSubWorkflowParent(
            String parentRun, String parentNode, boolean succeeded, @Nullable String error) {
        backend.resolveWorkflowGate(parentRun, parentNode, succeeded, error);
        RunPlan plan = plans.computeIfAbsent(parentRun, this::loadPlan);
        if (plan != null) {
            promoteSuccessors(parentRun, parentNode, plan);
        }
        finalizeIfTerminal(parentRun);
    }

    /** Drop all per-run state once a run is terminal, cancelling any lingering timers. */
    void forget(String runId) {
        plans.remove(runId);
        runNames.remove(runId);
        gateTimers.keySet().removeIf(key -> {
            if (key.runId.equals(runId)) {
                ScheduledFuture<?> timer = gateTimers.get(key);
                if (timer != null) {
                    timer.cancel(false);
                }
                return true;
            }
            return false;
        });
        resolvedGates.removeIf(key -> key.runId.equals(runId));
        promotedNodes.removeIf(key -> key.runId.equals(runId));
        childToParent.remove(runId);
        childToParent.values().removeIf(parent -> parent[0].equals(runId));
        childRunPayloads.remove(runId);
    }

    /** Stop the gate-timeout scheduler. Called when the owning worker closes. */
    public void close() {
        gateScheduler.shutdownNow();
    }

    /**
     * Read a job's serialized result, briefly polling in case the outcome event
     * outran the DB write. A missing result after the window is fatal to the
     * step: returning null here would silently drop a fan-out expansion or fan in
     * a {@code null} element, so we fail loudly instead.
     */
    private byte[] fetchResult(String jobId) {
        for (int attempt = 0; attempt < 50; attempt++) {
            Optional<byte[]> result = backend.getResult(jobId);
            if (result.isPresent()) {
                return result.get();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new WorkflowException("interrupted awaiting result for workflow job " + jobId, e);
            }
        }
        throw new WorkflowException("no result for workflow job " + jobId + " after 5s");
    }

    private Map<String, NodeSnapshot> statusMap(String runId) {
        WorkflowStatus status = backend.getWorkflowStatusJson(runId)
                .map(raw -> decode(raw, WorkflowStatus.class))
                .orElse(null);
        if (status == null) {
            return Map.of();
        }
        Map<String, NodeSnapshot> byName = new HashMap<>();
        for (NodeSnapshot node : status.nodes) {
            byName.put(node.nodeName, node);
        }
        return byName;
    }

    private static boolean allTerminal(List<String> predecessors, Map<String, NodeSnapshot> statuses) {
        for (String pred : predecessors) {
            NodeSnapshot snap = statuses.get(pred);
            if (snap == null || !isTerminal(snap.status)) {
                return false;
            }
        }
        return true;
    }

    private static boolean allCompleted(List<String> predecessors, Map<String, NodeSnapshot> statuses) {
        for (String pred : predecessors) {
            NodeSnapshot snap = statuses.get(pred);
            if (snap == null || !isCompleted(snap.status)) {
                return false;
            }
        }
        return true;
    }

    private static boolean anyFailed(List<String> predecessors, Map<String, NodeSnapshot> statuses) {
        for (String pred : predecessors) {
            NodeSnapshot snap = statuses.get(pred);
            if (snap != null && snap.status == NodeStatus.FAILED) {
                return true;
            }
        }
        return false;
    }

    private static boolean allNodesTerminal(Map<String, NodeSnapshot> statuses) {
        for (NodeSnapshot node : statuses.values()) {
            if (!isTerminal(node.status)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTerminal(NodeStatus status) {
        switch (status) {
            case COMPLETED:
            case FAILED:
            case SKIPPED:
            case CACHE_HIT:
            case COMPENSATED:
            case COMPENSATION_FAILED:
                return true;
            default:
                return false;
        }
    }

    private static boolean isCompleted(NodeStatus status) {
        return status == NodeStatus.COMPLETED || status == NodeStatus.CACHE_HIT;
    }

    private @Nullable RunPlan loadPlan(String runId) {
        return backend.getWorkflowPlanJson(runId)
                .map(raw -> RunPlan.from(decodeList(raw)))
                .orElse(null);
    }

    private <T> T decode(String raw, Class<T> type) {
        try {
            return json.readValue(raw, type);
        } catch (Exception e) {
            throw new SerializationException("failed to decode workflow data", e);
        }
    }

    private List<PlanNode> decodeList(String raw) {
        JavaType type = json.getTypeFactory().constructCollectionType(List.class, PlanNode.class);
        try {
            return json.readValue(raw, type);
        } catch (Exception e) {
            throw new SerializationException("failed to decode workflow plan", e);
        }
    }

    private static Thread daemon(Runnable runnable) {
        Thread thread = new Thread(runnable, "taskito-workflow-gates");
        thread.setDaemon(true);
        return thread;
    }

    /** A run + node identity used to key gate timers and dedupe promotion. */
    private static final class GateKey {
        final String runId;
        final String nodeName;

        GateKey(String runId, String nodeName) {
            this.runId = runId;
            this.nodeName = nodeName;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof GateKey)) {
                return false;
            }
            GateKey that = (GateKey) other;
            return runId.equals(that.runId) && nodeName.equals(that.nodeName);
        }

        @Override
        public int hashCode() {
            return 31 * runId.hashCode() + nodeName.hashCode();
        }
    }

    /** {@code {ttlMs}} parsed from a node's cache metadata. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class CacheMeta {
        final Long ttlMs;

        @JsonCreator
        CacheMeta(@JsonProperty("ttlMs") Long ttlMs) {
            this.ttlMs = ttlMs;
        }
    }

    /** {@code {timeoutMs, onTimeout, message}} parsed from a node's gate metadata. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class GateMeta {
        final Long timeoutMs;
        final String onTimeout;

        @JsonCreator
        GateMeta(
                @JsonProperty("timeoutMs") Long timeoutMs,
                @JsonProperty("onTimeout") String onTimeout,
                @JsonProperty("message") String message) {
            this.timeoutMs = timeoutMs;
            this.onTimeout = onTimeout;
        }
    }

    /** A serialized child workflow carried in a sub-workflow node's metadata. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class ChildSpec {
        final String name;
        final int version;
        final String stepsJson;
        final List<String> deferred;
        final Map<String, String> payloads;

        @JsonCreator
        ChildSpec(
                @JsonProperty("name") String name,
                @JsonProperty("version") int version,
                @JsonProperty("stepsJson") String stepsJson,
                @JsonProperty("deferred") List<String> deferred,
                @JsonProperty("payloads") Map<String, String> payloads) {
            this.name = name;
            this.version = version;
            this.stepsJson = stepsJson;
            this.deferred = deferred == null ? List.of() : deferred;
            this.payloads = payloads == null ? Map.of() : payloads;
        }
    }

    /** {@code {runId, nodeName}} from the native node-ref view. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class NodeRef {
        final String runId;
        final String nodeName;

        @JsonCreator
        NodeRef(@JsonProperty("runId") String runId, @JsonProperty("nodeName") String nodeName) {
            this.runId = runId;
            this.nodeName = nodeName;
        }
    }

    /** {@code {succeeded, childJobIds}} from the native fan-out completion view. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class FanOutCompletion {
        final boolean succeeded;
        final List<String> childJobIds;

        @JsonCreator
        FanOutCompletion(
                @JsonProperty("succeeded") boolean succeeded, @JsonProperty("childJobIds") List<String> childJobIds) {
            this.succeeded = succeeded;
            this.childJobIds = childJobIds == null ? List.of() : childJobIds;
        }
    }
}
