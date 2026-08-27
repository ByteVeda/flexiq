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
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param name the definition's name
     * @param version the definition's version
     * @param stepsJson the DAG's steps as JSON
     * @param payloadNames the node names {@code payloads} lines up with
     * @param payloads one encoded payload per named node
     * @param queueDefault the queue steps fall back to, or {@code null}
     * @param paramsJson the run's input parameters as JSON, or {@code null}
     * @param deferredNames nodes whose job is created later, not at submit
     * @param parentRunId the run spawning this one as a child, or {@code null} at the top level
     * @param parentNodeName the parent's node that spawned it, or {@code null}
     * @return the new run's id
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

    /**
     * Record a node's terminal outcome; returns the run's final state, or {@code null}.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param jobId the job's id
     * @param succeeded whether the node's job completed rather than dead-lettered
     * @param error why it failed, or {@code null}
     * @param skipCascade {@code true} to leave dependent nodes alone rather than skipping them
     * @return the run's final state, or {@code null} while it is still live
     */
    public static native String markWorkflowNodeResult(
            long handle, String jobId, boolean succeeded, @Nullable String error, boolean skipCascade);

    /**
     * Returns a JSON run + node snapshot, or {@code null} if the run is absent.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @return a JSON run + node snapshot, or {@code null}
     */
    public static native String getWorkflowStatus(long handle, String runId);

    /**
     * A page of workflow runs.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param definitionNameOrNull narrow to one definition, or {@code null} for every one
     * @param stateOrNull narrow to one state, or {@code null} for every state
     * @param limit the page size
     * @param offset how many runs to skip
     * @return the runs as a JSON array
     */
    public static native String listWorkflowRuns(
            long handle, @Nullable String definitionNameOrNull, @Nullable String stateOrNull, long limit, long offset);

    /**
     * A run's summary row, without node detail.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @return the run as JSON, or {@code null}
     */
    public static native String getWorkflowRun(long handle, String runId);

    /**
     * The sub-workflow runs a run spawned.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @return the children as a JSON array
     */
    public static native String getWorkflowChildren(long handle, String runId);

    /**
     * A run's graph as the core stored it.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @return the graph as JSON, or {@code null}
     */
    public static native String getWorkflowDag(long handle, String runId);

    /**
     * Cancel a run and every node still pending.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     */
    public static native void cancelWorkflowRun(long handle, String runId);

    // ── Fan-out / fan-in orchestration (driven by the worker-side tracker) ──

    /**
     * Returns the run's nodes with predecessors + step metadata (JSON), or {@code null}.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @return the plan as JSON, or {@code null}
     */
    public static native String getWorkflowPlan(long handle, String runId);

    /**
     * Returns {@code {runId, nodeName}} for a job (JSON), or {@code null} if non-workflow.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param jobId the job's id
     * @return {@code {runId, nodeName}} as JSON, or {@code null}
     */
    public static native String workflowNodeForJob(long handle, String jobId);

    /**
     * Returns the run's definition name, or {@code null} if the run is absent.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @return the definition's name, or {@code null}
     */
    public static native String workflowNameForRun(long handle, String runId);

    /**
     * Expand a fan-out parent into one child job per payload; returns the child job ids.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param parentNode the fan-out node whose result was split
     * @param childNames the node names to create, one per item
     * @param childPayloads one encoded payload per child, in the same order
     * @param taskName the task's registered name
     * @param queue the queue the jobs go to
     * @param maxRetries the retry ceiling for each job
     * @param timeoutMs the per-attempt timeout for each job
     * @param priority the dispatch priority for each job
     * @return the created job ids, in child order
     */
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

    /**
     * Returns {@code {succeeded, childJobIds}} once all children settle (JSON), else {@code null}.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param parentNode the fan-out node whose result was split
     * @return the aggregate as JSON, or {@code null} while children are outstanding
     */
    public static native String checkFanOutCompletion(long handle, String runId, String parentNode);

    /**
     * Enqueue a job for a deferred node (e.g. the fan-in collector); returns the job id.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param payload the encoded payload, resolved now rather than at submit
     * @param taskName the task's registered name
     * @param queue the queue the jobs go to
     * @param maxRetries the retry ceiling for each job
     * @param timeoutMs the per-attempt timeout for each job
     * @param priority the dispatch priority for each job
     * @return the created job's id
     */
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

    /**
     * Skip every node still pending, after a failure that ends the run.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     */
    public static native void cascadeSkipPending(long handle, String runId);

    /**
     * Finalize the run if every node is terminal; returns the final state, or {@code null}.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @return the final state, or {@code null} while nodes are outstanding
     */
    public static native String finalizeRunIfTerminal(long handle, String runId);

    // ── Gates / conditional nodes (driven by the worker-side tracker) ──

    /**
     * Park an approval-gate node until it is resolved.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    public static native void setWorkflowNodeWaitingApproval(long handle, String runId, String nodeName);

    /**
     * Settle a parked gate (or sub-workflow parent): completed if approved, else failed with {@code error}.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param approved {@code true} to complete the node, {@code false} to fail it
     * @param error why it failed, or {@code null}
     */
    public static native void resolveWorkflowGate(
            long handle, String runId, String nodeName, boolean approved, @Nullable String error);

    /**
     * Promote a gate / sub-workflow node to running.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    public static native void setWorkflowNodeRunning(long handle, String runId, String nodeName);

    /**
     * Mark a node failed (e.g. a sub-workflow whose child could not be submitted).
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param error why it failed, or {@code null}
     */
    public static native void failWorkflowNode(long handle, String runId, String nodeName, @Nullable String error);

    /**
     * Mark a node skipped (its condition evaluated false) and cancel any bound job.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    public static native void skipWorkflowNode(long handle, String runId, String nodeName);

    /**
     * Mark a node as a cache hit (terminal, treated as completed) without running it.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    public static native void setWorkflowNodeCacheHit(long handle, String runId, String nodeName);

    // ── Saga compensation (driven by the worker-side tracker) ──

    /**
     * Move a failed run into rollback.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     */
    public static native void setWorkflowRunCompensating(long handle, String runId);

    /**
     * Record that a run's rollback finished cleanly.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param completedAt when it settled, in Unix milliseconds
     */
    public static native void setWorkflowRunCompensated(long handle, String runId, long completedAt);

    /**
     * Record that a run's rollback itself failed.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param completedAt when it settled, in Unix milliseconds
     * @param error why it failed, or {@code null}
     */
    public static native void setWorkflowRunCompensationFailed(
            long handle, String runId, long completedAt, @Nullable String error);

    /**
     * Settle a run that finished with some nodes failed or skipped.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param completedAt when it settled, in Unix milliseconds
     */
    public static native void setWorkflowRunCompletedWithFailures(long handle, String runId, long completedAt);

    /**
     * Bind a node to the job that is rolling it back.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param compensationJobId the rollback job's id
     * @param startedAt when the rollback was dispatched, in Unix milliseconds
     */
    public static native void setWorkflowNodeCompensationJob(
            long handle, String runId, String nodeName, String compensationJobId, long startedAt);

    /**
     * Record that one node was rolled back cleanly.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param completedAt when it settled, in Unix milliseconds
     */
    public static native void setWorkflowNodeCompensated(long handle, String runId, String nodeName, long completedAt);

    /**
     * Record that one node's rollback failed.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param error why it failed, or {@code null}
     * @param completedAt when it settled, in Unix milliseconds
     */
    public static native void setWorkflowNodeCompensationFailed(
            long handle, String runId, String nodeName, @Nullable String error, long completedAt);
}
