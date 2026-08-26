package org.byteveda.flexiq.spi;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Low-level queue operations a backend provides, in native-shaped terms (opaque
 * {@code byte[]} payloads, JSON strings for options and views).
 *
 * <p>This is the seam between the public API and its implementation. The default
 * implementation is JNI-backed; alternatives (an FFM backend, or an in-memory
 * fake for tests) can be supplied without touching the public API. Methods that
 * return a JSON collection never return {@code null}; nullable scalars are
 * returned as {@link Optional}.
 */
public interface QueueBackend extends AutoCloseable, ConditionalSettings {
    // ── Producer ────────────────────────────────────────────────────

    /** Enqueue one job; {@code optionsJson} is a single {@code EnqueueOptions} object. */
    String enqueue(String taskName, byte[] payload, String optionsJson);

    /**
     * Enqueue a batch. Unlike {@link #enqueue}, {@code optionsJson} is a JSON
     * <em>array</em> of per-job {@code EnqueueOptions}, the same length as
     * {@code payloads}. Returns the job ids in input order.
     */
    String[] enqueueMany(String taskName, byte[][] payloads, String optionsJson);

    Optional<String> getJobJson(String jobId);

    Optional<byte[]> getResult(String jobId);

    boolean cancel(String jobId);

    boolean requestCancel(String jobId);

    boolean isCancelRequested(String jobId);

    void setProgress(String jobId, int progress);

    // ── Inspection ──────────────────────────────────────────────────
    String statsJson();

    String statsByQueueJson(String queue);

    /**
     * Count pending jobs on {@code queue} — the primitive behind the
     * {@code maxPending} cap. Defaults to unsupported so this optional capability
     * doesn't break existing third-party {@code QueueBackend} implementations at
     * compile time; it is only invoked when a queue actually has a cap set.
     */
    default long countPendingByQueue(String queue) {
        throw new UnsupportedOperationException("countPendingByQueue not supported by this backend");
    }

    String statsAllQueuesJson();

    String listJobsJson(String filterJson);

    /**
     * A keyset-paginated page of jobs as JSON. Defaults to unsupported: a backend
     * without a seekable ordering cannot honour the cursor contract, and silently
     * returning an unpaginated list would look like the last page.
     */
    default String listJobsAfterJson(String filterJson, @Nullable String afterOrNull) {
        throw new UnsupportedOperationException("keyset pagination not supported by this backend");
    }

    /** A keyset-paginated page of archived jobs as JSON. See {@link #listJobsAfterJson}. */
    default String listArchivedAfterJson(long limit, @Nullable String afterOrNull) {
        throw new UnsupportedOperationException("keyset pagination not supported by this backend");
    }

    String jobErrorsJson(String jobId);

    String metricsJson(@Nullable String taskNameOrNull, long sinceMs);

    String listWorkersJson();

    /** Circuit-breaker states as a JSON array; defaults to none for backends without breakers. */
    default String listCircuitBreakersJson() {
        return "[]";
    }

    // ── Admin ───────────────────────────────────────────────────────
    String listDeadJson(long limit, long offset);

    String retryDead(String deadId);

    /** Re-enqueue a copy of a job and return the new id. */
    default String replayJob(String jobId) {
        throw new UnsupportedOperationException("replay not supported by this backend");
    }

    /** A job's replay history; defaults to none. */
    default String getReplayHistoryJson(String jobId) {
        return "[]";
    }

    /** A job's dependency DAG; defaults to just the empty graph. */
    default String jobDagJson(String jobId) {
        return "{\"nodes\":[],\"edges\":[]}";
    }

    boolean deleteDead(String deadId);

    /**
     * Force a stuck running job back to pending, releasing its execution claim.
     * Returns false when the job is missing or is not running.
     */
    default boolean requeueJob(String jobId) {
        throw new UnsupportedOperationException("requeue is not supported by this backend");
    }

    long purgeDead(long olderThanMs);

    /** A JSON array of dead-letter entries for one task. */
    default String listDeadByTaskJson(String taskName, long limit, long offset) {
        throw new UnsupportedOperationException("per-task dead-letter queries not supported by this backend");
    }

    /** Delete every dead-letter entry for a task; returns the number removed. */
    default long purgeDeadByTask(String taskName) {
        throw new UnsupportedOperationException("per-task dead-letter queries not supported by this backend");
    }

    long purgeCompleted(long olderThanMs);

    void pauseQueue(String queue);

    void resumeQueue(String queue);

    String listPausedQueuesJson();

    void setSetting(String key, String value);

    /**
     * {@inheritDoc}
     *
     * <p>Defaults to a <b>non-atomic</b> read-compare-write over
     * {@link #getSetting} and {@link #setSetting}, so a backend written before
     * this method existed keeps compiling and behaves exactly as it did — a
     * concurrent writer between the read and the write is still lost. Override
     * it with whatever your storage can do atomically.
     */
    @Override
    default boolean setSettingIf(String key, Optional<String> expected, String value) {
        if (!getSetting(key).equals(expected)) {
            return false;
        }
        setSetting(key, value);
        return true;
    }

    boolean deleteSetting(String key);

    String listSettingsJson();

    /**
     * Applies pending schema changes, returning the report as JSON. Only the
     * native backend owns a schema, so only it can migrate one.
     */
    default String migrateJson() {
        throw new UnsupportedOperationException("migrate requires the native backend");
    }

    /**
     * The lowest contract level a process may speak and still open this
     * storage. The level itself is a property of the native build, so only the
     * native backend can answer.
     */
    default int minContract() {
        throw new UnsupportedOperationException("the contract floor requires the native backend");
    }

    /** Raises or lowers that floor. Native-only, for the same reason. */
    default void setMinContract(int level) {
        throw new UnsupportedOperationException("the contract floor requires the native backend");
    }

    /**
     * The retention windows the elected cleaner last published for this queue's
     * namespace, as JSON, or empty when no worker has swept yet. Defaults to
     * empty for backends that do not report one.
     */
    default Optional<String> effectiveRetentionJson() {
        return Optional.empty();
    }

    /**
     * Preview counts as JSON for the given candidate retention spec (camelCase
     * seconds), or {@code null} for the recommended defaults. Only the native
     * backend can count against live storage, so non-native backends do not
     * support the dry-run.
     */
    default String dryRunRetentionJson(@Nullable String retentionJson) {
        throw new UnsupportedOperationException("retention dry-run requires the native backend");
    }

    // ── Logs ────────────────────────────────────────────────────────
    void writeTaskLog(String jobId, String taskName, String level, String message, @Nullable String extraOrNull);

    String getTaskLogsJson(String jobId);

    /** Logs for a job after a cursor id; defaults to none for backends without cursor support. */
    default String getTaskLogsAfterJson(String jobId, @Nullable String afterIdOrNull) {
        return "[]";
    }

    /** Logs across jobs filtered by task/level/since; defaults to none. */
    default String queryTaskLogsJson(
            @Nullable String taskNameOrNull, @Nullable String levelOrNull, long sinceMs, long limit) {
        return "[]";
    }

    /**
     * Whether this backend has a durable-step store.
     *
     * <p>A capability probe, not the gate: a step session refuses on its own if
     * the store is missing, and mirroring that rule here is how a shell drifts
     * from the core. This exists for an application that wants the answer
     * without dispatching a job first.
     *
     * <p>Defaults to {@code false}, so a custom backend that has never heard of
     * {@code job_steps} reports the truth rather than promising a store it does
     * not have.
     */
    default boolean supportsSteps() {
        return false;
    }

    // ── Locks ───────────────────────────────────────────────────────
    // Optional capability: default to throwing so existing custom backends keep
    // compiling and fail explicitly only when locks are actually used.
    default boolean acquireLock(String name, String ownerId, long ttlMs) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    default boolean releaseLock(String name, String ownerId) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    default boolean extendLock(String name, String ownerId, long ttlMs) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    default Optional<String> lockInfoJson(String name) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    // ── Periodic ────────────────────────────────────────────────────
    default long registerPeriodic(
            String name,
            String taskName,
            String cron,
            byte @Nullable [] args,
            @Nullable String queue,
            @Nullable String timezone,
            boolean enabled) {
        throw new UnsupportedOperationException("periodic tasks not supported by this backend");
    }

    /** A JSON array of every registered periodic task. */
    default String listPeriodicJson() {
        throw new UnsupportedOperationException("periodic tasks not supported by this backend");
    }

    /** Remove a periodic task; false if none had that name. */
    default boolean deletePeriodic(String name) {
        throw new UnsupportedOperationException("periodic tasks not supported by this backend");
    }

    /** Pause (false) or resume (true) a periodic task; false if none had that name. */
    default boolean setPeriodicEnabled(String name, boolean enabled) {
        throw new UnsupportedOperationException("periodic tasks not supported by this backend");
    }

    // ── Pub/Sub ─────────────────────────────────────────────────────
    // Optional capability: default to throwing so existing custom backends keep
    // compiling and fail explicitly only when pub/sub is actually used.
    String PUBSUB_UNSUPPORTED = "pub/sub not supported by this backend";

    /**
     * Insert or update a topic subscription with no per-subscriber delivery
     * settings; deliveries take the queue defaults. This is the base method a
     * backend overrides — the {@code Integer/Integer/Long} overload delegates
     * here so a backend that only implements this form keeps working.
     */
    default void registerSubscription(
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            @Nullable String ownerWorkerIdOrNull) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Insert or update a topic subscription (idempotent on topic + name).
     * Re-registering updates routing but preserves a paused state; an ephemeral
     * subscription ({@code durable=false}) requires {@code ownerWorkerIdOrNull}.
     *
     * <p>{@code priority}, {@code maxRetries}, and {@code timeoutMs} are the
     * subscriber task's own delivery settings, persisted on the row so a
     * producer-only process applies them without loading the task; {@code null}
     * means "take the queue default".
     *
     * <p>The default drops the three settings and delegates to the six-argument
     * form, so a backend that predates delivery-setting persistence still
     * registers the subscription (it just takes queue defaults) instead of
     * throwing.
     */
    default void registerSubscription(
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            @Nullable String ownerWorkerIdOrNull,
            @Nullable Integer priority,
            @Nullable Integer maxRetries,
            @Nullable Long timeoutMs) {
        registerSubscription(topic, subscriptionName, taskName, queue, durable, ownerWorkerIdOrNull);
    }

    /**
     * Insert or update a subscription with an explicit delivery {@code mode}:
     * {@code "fanout"} (one job per publish) or {@code "log"} (append-once + a
     * per-subscription cursor pulled via {@link #readTopicMessagesJson}).
     *
     * <p>The default preserves fan-out compatibility for backends that predate
     * log topics — {@code "fanout"} delegates to the mode-less form — but rejects
     * {@code "log"} rather than silently downgrading it to fan-out.
     */
    default void registerSubscription(
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            @Nullable String ownerWorkerIdOrNull,
            @Nullable Integer priority,
            @Nullable Integer maxRetries,
            @Nullable Long timeoutMs,
            String mode) {
        if (!"fanout".equals(mode)) {
            throw new UnsupportedOperationException("log topics not supported by this backend");
        }
        registerSubscription(
                topic,
                subscriptionName,
                taskName,
                queue,
                durable,
                ownerWorkerIdOrNull,
                priority,
                maxRetries,
                timeoutMs);
    }

    /** A JSON array of subscriptions — all of them, or only a topic's active ones. */
    default String listSubscriptionsJson(@Nullable String topicOrNull) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** Remove a subscription; false if none matched. */
    default boolean unsubscribe(String topic, String subscriptionName) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** Pause (false) or resume (true) a subscription; false if none matched. */
    default boolean setSubscriptionActive(String topic, String subscriptionName, boolean active) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** A JSON array of per-subscription backlog snapshots, one per registered subscription. */
    default String topicBacklogStatsJson() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** Drop ephemeral subscriptions whose owning worker is gone; returns the count removed. */
    default long reapEphemeralSubscriptions() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Fan a payload out to every active subscription of {@code topic}. Returns
     * the created jobs as a JSON array — empty when nothing is subscribed.
     */
    default String publishJson(String topic, byte[] payload, String optionsJson) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Pull up to {@code limit} messages after a log subscription's cursor (oldest
     * first, exclusive), as a JSON array of message views. At-least-once: reading
     * without acking re-delivers.
     */
    default String readTopicMessagesJson(String topic, String subscriptionName, long limit) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** Advance a log subscription's cursor to {@code cursor} (monotonic); false if nothing moved. */
    default boolean ackTopicCursor(String topic, String subscriptionName, String cursor) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Lease up to {@code limit} available messages for {@code visibilityMs}, tracking
     * per-message state, as a JSON array of message views. A nack or an expired lease
     * redelivers just that message without blocking its siblings.
     */
    default String leaseTopicMessagesJson(String topic, String subscriptionName, long limit, long visibilityMs) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** Ack one leased message; false if there was no un-acked delivery to ack. */
    default boolean ackMessage(String topic, String subscriptionName, String messageId) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** Nack one leased message (redeliver now); false if there was no un-acked delivery to nack. */
    default boolean nackMessage(String topic, String subscriptionName, String messageId) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** A JSON array of per-log-subscription lag snapshots. */
    default String topicLogStatsJson() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Declare a log topic (idempotent) so its publishes are retained even with no
     * subscriber. {@code retentionMs} bounds a sub-less backlog; {@code null}
     * keeps messages until consumed.
     */
    default void declareTopic(String name, @Nullable Long retentionMs) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /** A JSON array of declared topics. */
    default String listDeclaredTopicsJson() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    // ── Workflows ───────────────────────────────────────────────────
    // Optional capability: default to throwing so existing custom backends keep
    // compiling and fail explicitly only when workflows are actually used.
    String WORKFLOWS_UNSUPPORTED = "workflows not supported by this backend";

    default String submitWorkflow(
            String name,
            int version,
            String stepsJson,
            String[] payloadNames,
            byte[][] payloads,
            @Nullable String queueDefault,
            @Nullable String paramsJson,
            String[] deferredNames,
            @Nullable String parentRunId,
            @Nullable String parentNodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Record a node's terminal outcome; returns the run's final state, or {@code null}. */
    default String markWorkflowNodeResult(
            String jobId, boolean succeeded, @Nullable String error, boolean skipCascade) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default Optional<String> getWorkflowStatusJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default String listWorkflowRunsJson(
            @Nullable String definitionNameOrNull, @Nullable String stateOrNull, long limit, long offset) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default Optional<String> getWorkflowRunJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default String getWorkflowChildrenJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default Optional<String> getWorkflowDagJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void cancelWorkflowRun(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default Optional<String> getWorkflowPlanJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default Optional<String> workflowNodeForJobJson(String jobId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Returns the run's definition name, or empty if the run is absent. */
    default Optional<String> workflowNameForRun(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default String[] expandFanOut(
            String runId,
            String parentNode,
            String[] childNames,
            byte[][] childPayloads,
            String taskName,
            String queue,
            int maxRetries,
            long timeoutMs,
            int priority) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default Optional<String> checkFanOutCompletionJson(String runId, String parentNode) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default String createDeferredJob(
            String runId,
            String nodeName,
            byte[] payload,
            String taskName,
            String queue,
            int maxRetries,
            long timeoutMs,
            int priority) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void cascadeSkipPending(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default Optional<String> finalizeRunIfTerminal(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Park an approval-gate node until it is resolved. */
    default void setWorkflowNodeWaitingApproval(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Settle a parked gate: completed if approved, else failed with {@code error}. */
    default void resolveWorkflowGate(String runId, String nodeName, boolean approved, @Nullable String error) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Promote a gate / sub-workflow node to running. */
    default void setWorkflowNodeRunning(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Mark a node failed. */
    default void failWorkflowNode(String runId, String nodeName, @Nullable String error) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Mark a node skipped (its condition evaluated false) and cancel any bound job. */
    default void skipWorkflowNode(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /** Mark a node as a cache hit (terminal) without running it. */
    default void setWorkflowNodeCacheHit(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    // ── Saga compensation ─────────────────────────────────────────

    default void setWorkflowRunCompensating(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void setWorkflowRunCompensated(String runId, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void setWorkflowRunCompensationFailed(String runId, long completedAt, @Nullable String error) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void setWorkflowRunCompletedWithFailures(String runId, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void setWorkflowNodeCompensationJob(
            String runId, String nodeName, String compensationJobId, long startedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void setWorkflowNodeCompensated(String runId, String nodeName, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    default void setWorkflowNodeCompensationFailed(
            String runId, String nodeName, @Nullable String error, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    // ── Worker ──────────────────────────────────────────────────────
    /** Start a worker that dispatches jobs to {@code bridge}; returns its control. */
    WorkerControl startWorker(WorkerBridge bridge, String optionsJson);

    @Override
    void close();
}
