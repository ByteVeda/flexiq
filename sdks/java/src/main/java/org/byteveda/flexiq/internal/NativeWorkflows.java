package org.byteveda.flexiq.internal;

import org.jspecify.annotations.Nullable;

/**
 * JNI surface for workflow operations (the native {@code workflows} feature).
 *
 * <p>Opaque payloads cross as {@code byte[]}; step descriptions and views cross
 * as JSON strings. Methods throw {@link org.byteveda.flexiq.FlexiQException}
 * on native failure. The {@code handle} is the queue handle from
 * {@link NativeQueue#open}.
 */
public final class NativeWorkflows {
    static {
        NativeLoader.load();
    }

    private NativeWorkflows() {}

    /**
     * Record a run and pre-enqueue a job per static step; returns the run id.
     * {@code parentRunId}/{@code parentNodeName} link a sub-workflow child to its
     * parent node (both {@code null} for a top-level run).
     */
    public static native String submitWorkflow(
            long handle,
            String name,
            int version,
            String stepsJson,
            String[] payloadNames,
            byte[][] payloads,
            @Nullable String queueDefault,
            @Nullable String paramsJson,
            String[] deferredNames,
            @Nullable String parentRunId,
            @Nullable String parentNodeName);

    /** Record a node's terminal outcome; returns the run's final state, or {@code null}. */
    public static native String markWorkflowNodeResult(
            long handle, String jobId, boolean succeeded, @Nullable String error, boolean skipCascade);

    /** Returns a JSON run + node snapshot, or {@code null} if the run is absent. */
    public static native String getWorkflowStatus(long handle, String runId);

    public static native String listWorkflowRuns(
            long handle, @Nullable String definitionNameOrNull, @Nullable String stateOrNull, long limit, long offset);

    public static native String getWorkflowRun(long handle, String runId);

    public static native String getWorkflowChildren(long handle, String runId);

    public static native String getWorkflowDag(long handle, String runId);

    public static native void cancelWorkflowRun(long handle, String runId);

    // ── Fan-out / fan-in orchestration (driven by the worker-side tracker) ──

    /** Returns the run's nodes with predecessors + step metadata (JSON), or {@code null}. */
    public static native String getWorkflowPlan(long handle, String runId);

    /** Returns {@code {runId, nodeName}} for a job (JSON), or {@code null} if non-workflow. */
    public static native String workflowNodeForJob(long handle, String jobId);

    /** Returns the run's definition name, or {@code null} if the run is absent. */
    public static native String workflowNameForRun(long handle, String runId);

    /** Expand a fan-out parent into one child job per payload; returns the child job ids. */
    public static native String[] expandFanOut(
            long handle,
            String runId,
            String parentNode,
            String[] childNames,
            byte[][] childPayloads,
            String taskName,
            String queue,
            int maxRetries,
            long timeoutMs,
            int priority);

    /** Returns {@code {succeeded, childJobIds}} once all children settle (JSON), else {@code null}. */
    public static native String checkFanOutCompletion(long handle, String runId, String parentNode);

    /** Enqueue a job for a deferred node (e.g. the fan-in collector); returns the job id. */
    public static native String createDeferredJob(
            long handle,
            String runId,
            String nodeName,
            byte[] payload,
            String taskName,
            String queue,
            int maxRetries,
            long timeoutMs,
            int priority);

    public static native void cascadeSkipPending(long handle, String runId);

    /** Finalize the run if every node is terminal; returns the final state, or {@code null}. */
    public static native String finalizeRunIfTerminal(long handle, String runId);

    // ── Gates / conditional nodes (driven by the worker-side tracker) ──

    /** Park an approval-gate node until it is resolved. */
    public static native void setWorkflowNodeWaitingApproval(long handle, String runId, String nodeName);

    /** Settle a parked gate (or sub-workflow parent): completed if approved, else failed with {@code error}. */
    public static native void resolveWorkflowGate(
            long handle, String runId, String nodeName, boolean approved, @Nullable String error);

    /** Promote a gate / sub-workflow node to running. */
    public static native void setWorkflowNodeRunning(long handle, String runId, String nodeName);

    /** Mark a node failed (e.g. a sub-workflow whose child could not be submitted). */
    public static native void failWorkflowNode(long handle, String runId, String nodeName, @Nullable String error);

    /** Mark a node skipped (its condition evaluated false) and cancel any bound job. */
    public static native void skipWorkflowNode(long handle, String runId, String nodeName);

    /** Mark a node as a cache hit (terminal, treated as completed) without running it. */
    public static native void setWorkflowNodeCacheHit(long handle, String runId, String nodeName);

    // ── Saga compensation (driven by the worker-side tracker) ──

    public static native void setWorkflowRunCompensating(long handle, String runId);

    public static native void setWorkflowRunCompensated(long handle, String runId, long completedAt);

    public static native void setWorkflowRunCompensationFailed(
            long handle, String runId, long completedAt, @Nullable String error);

    public static native void setWorkflowRunCompletedWithFailures(long handle, String runId, long completedAt);

    public static native void setWorkflowNodeCompensationJob(
            long handle, String runId, String nodeName, String compensationJobId, long startedAt);

    public static native void setWorkflowNodeCompensated(long handle, String runId, String nodeName, long completedAt);

    public static native void setWorkflowNodeCompensationFailed(
            long handle, String runId, String nodeName, @Nullable String error, long completedAt);
}
