package org.byteveda.flexiq.internal;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.byteveda.flexiq.spi.QueueBackend;
import org.byteveda.flexiq.spi.WorkerBridge;
import org.byteveda.flexiq.spi.WorkerControl;
import org.jspecify.annotations.Nullable;

/**
 * JNI-backed {@link QueueBackend} over a native queue handle. Every call holds
 * the read lock; {@code close()} takes the write lock, so it waits out in-flight
 * calls and later calls throw {@link IllegalStateException} instead of touching
 * freed native memory.
 */
public final class JniQueueBackend implements QueueBackend {
    private final long handle;

    /** Hot byte ops (enqueue/enqueueMany/getResult) route through this; FFM when available, else JNI. */
    private final NativeTransport transport;

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private boolean closed; // guarded by stateLock

    private JniQueueBackend(long handle) {
        this.handle = handle;
        this.transport = NativeTransport.create(handle);
    }

    /** Open a native backend from its JSON options. */
    public static JniQueueBackend open(String optionsJson) {
        return new JniQueueBackend(NativeQueue.open(optionsJson));
    }

    private <T> T withOpenHandle(Supplier<T> nativeCall) {
        stateLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("queue backend is closed");
            }
            return nativeCall.get();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public String enqueue(String taskName, byte[] payload, String optionsJson) {
        return withOpenHandle(() -> transport.enqueue(taskName, payload, optionsJson));
    }

    @Override
    public String[] enqueueMany(String taskName, byte[][] payloads, String optionsJson) {
        return withOpenHandle(() -> transport.enqueueMany(taskName, payloads, optionsJson));
    }

    @Override
    public Optional<String> getJobJson(String jobId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeQueue.getJob(handle, jobId)));
    }

    @Override
    public Optional<byte[]> getResult(String jobId) {
        return withOpenHandle(() -> Optional.ofNullable(transport.getResult(jobId)));
    }

    @Override
    public boolean cancel(String jobId) {
        return withOpenHandle(() -> NativeQueue.cancel(handle, jobId));
    }

    @Override
    public boolean requestCancel(String jobId) {
        return withOpenHandle(() -> NativeQueue.requestCancel(handle, jobId));
    }

    @Override
    public boolean isCancelRequested(String jobId) {
        return withOpenHandle(() -> NativeQueue.isCancelRequested(handle, jobId));
    }

    @Override
    public void setProgress(String jobId, int progress) {
        withOpenHandle(() -> {
            NativeQueue.setProgress(handle, jobId, progress);
            return null;
        });
    }

    @Override
    public String statsJson() {
        return withOpenHandle(() -> NativeQueue.stats(handle));
    }

    @Override
    public String statsByQueueJson(String queue) {
        return withOpenHandle(() -> NativeQueue.statsByQueue(handle, queue));
    }

    @Override
    public long countPendingByQueue(String queue) {
        return withOpenHandle(() -> NativeQueue.countPendingByQueue(handle, queue));
    }

    @Override
    public String statsAllQueuesJson() {
        return withOpenHandle(() -> NativeQueue.statsAllQueues(handle));
    }

    @Override
    public String listJobsJson(String filterJson) {
        return withOpenHandle(() -> NativeQueue.listJobs(handle, filterJson));
    }

    @Override
    public String listJobsAfterJson(String filterJson, @Nullable String afterOrNull) {
        return withOpenHandle(() -> NativeQueue.listJobsAfter(handle, filterJson, afterOrNull));
    }

    @Override
    public String listArchivedAfterJson(long limit, @Nullable String afterOrNull) {
        return withOpenHandle(() -> NativeQueue.listArchivedAfter(handle, limit, afterOrNull));
    }

    @Override
    public String jobErrorsJson(String jobId) {
        return withOpenHandle(() -> NativeQueue.jobErrors(handle, jobId));
    }

    @Override
    public String metricsJson(@Nullable String taskNameOrNull, long sinceMs) {
        return withOpenHandle(() -> NativeQueue.metrics(handle, taskNameOrNull, sinceMs));
    }

    @Override
    public String listWorkersJson() {
        return withOpenHandle(() -> NativeQueue.listWorkers(handle));
    }

    @Override
    public String listCircuitBreakersJson() {
        return withOpenHandle(() -> NativeQueue.listCircuitBreakers(handle));
    }

    @Override
    public String listDeadJson(long limit, long offset) {
        return withOpenHandle(() -> NativeQueue.listDead(handle, limit, offset));
    }

    @Override
    public String retryDead(String deadId) {
        return withOpenHandle(() -> NativeQueue.retryDead(handle, deadId));
    }

    @Override
    public String replayJob(String jobId) {
        return withOpenHandle(() -> NativeQueue.replayJob(handle, jobId));
    }

    @Override
    public String getReplayHistoryJson(String jobId) {
        return withOpenHandle(() -> NativeQueue.getReplayHistory(handle, jobId));
    }

    @Override
    public String jobDagJson(String jobId) {
        return withOpenHandle(() -> NativeQueue.jobDag(handle, jobId));
    }

    @Override
    public boolean deleteDead(String deadId) {
        return withOpenHandle(() -> NativeQueue.deleteDead(handle, deadId));
    }

    @Override
    public boolean requeueJob(String jobId) {
        return withOpenHandle(() -> NativeQueue.requeueJob(handle, jobId));
    }

    @Override
    public long purgeDead(long olderThanMs) {
        return withOpenHandle(() -> NativeQueue.purgeDead(handle, olderThanMs));
    }

    @Override
    public String listDeadByTaskJson(String taskName, long limit, long offset) {
        return withOpenHandle(() -> NativeQueue.listDeadByTask(handle, taskName, limit, offset));
    }

    @Override
    public long purgeDeadByTask(String taskName) {
        return withOpenHandle(() -> NativeQueue.purgeDeadByTask(handle, taskName));
    }

    @Override
    public long purgeCompleted(long olderThanMs) {
        return withOpenHandle(() -> NativeQueue.purgeCompleted(handle, olderThanMs));
    }

    @Override
    public void pauseQueue(String queue) {
        withOpenHandle(() -> {
            NativeQueue.pauseQueue(handle, queue);
            return null;
        });
    }

    @Override
    public void resumeQueue(String queue) {
        withOpenHandle(() -> {
            NativeQueue.resumeQueue(handle, queue);
            return null;
        });
    }

    @Override
    public String listPausedQueuesJson() {
        return withOpenHandle(() -> NativeQueue.listPausedQueues(handle));
    }

    @Override
    public Optional<String> getSetting(String key) {
        return withOpenHandle(() -> Optional.ofNullable(NativeQueue.getSetting(handle, key)));
    }

    @Override
    public void setSetting(String key, String value) {
        withOpenHandle(() -> {
            NativeQueue.setSetting(handle, key, value);
            return null;
        });
    }

    @Override
    public boolean setSettingIf(String key, Optional<String> expected, String value) {
        return withOpenHandle(() -> NativeQueue.setSettingIf(handle, key, expected.orElse(null), value));
    }

    @Override
    public boolean deleteSetting(String key) {
        return withOpenHandle(() -> NativeQueue.deleteSetting(handle, key));
    }

    @Override
    public Optional<String> effectiveRetentionJson() {
        return withOpenHandle(() -> Optional.ofNullable(NativeQueue.effectiveRetention(handle)));
    }

    @Override
    public String dryRunRetentionJson(@Nullable String retentionJson) {
        return withOpenHandle(() -> NativeQueue.dryRunRetention(handle, retentionJson));
    }

    @Override
    public String listSettingsJson() {
        return withOpenHandle(() -> NativeQueue.listSettings(handle));
    }

    @Override
    public String migrateJson() {
        return withOpenHandle(() -> NativeQueue.migrate(handle));
    }

    @Override
    public int minContract() {
        return withOpenHandle(() -> NativeQueue.minContract(handle));
    }

    @Override
    public void setMinContract(int level) {
        withOpenHandle(() -> {
            NativeQueue.setMinContract(handle, level);
            return null;
        });
    }

    @Override
    public void writeTaskLog(
            String jobId, String taskName, String level, String message, @Nullable String extraOrNull) {
        withOpenHandle(() -> {
            NativeQueue.writeTaskLog(handle, jobId, taskName, level, message, extraOrNull);
            return null;
        });
    }

    @Override
    public String getTaskLogsJson(String jobId) {
        return withOpenHandle(() -> NativeQueue.getTaskLogs(handle, jobId));
    }

    @Override
    public String getTaskLogsAfterJson(String jobId, @Nullable String afterIdOrNull) {
        return withOpenHandle(() -> NativeQueue.getTaskLogsAfter(handle, jobId, afterIdOrNull));
    }

    @Override
    public String queryTaskLogsJson(
            @Nullable String taskNameOrNull, @Nullable String levelOrNull, long sinceMs, long limit) {
        return withOpenHandle(() -> NativeQueue.queryTaskLogs(handle, taskNameOrNull, levelOrNull, sinceMs, limit));
    }

    @Override
    public boolean acquireLock(String name, String ownerId, long ttlMs) {
        return withOpenHandle(() -> NativeQueue.acquireLock(handle, name, ownerId, ttlMs));
    }

    @Override
    public boolean releaseLock(String name, String ownerId) {
        return withOpenHandle(() -> NativeQueue.releaseLock(handle, name, ownerId));
    }

    @Override
    public boolean extendLock(String name, String ownerId, long ttlMs) {
        return withOpenHandle(() -> NativeQueue.extendLock(handle, name, ownerId, ttlMs));
    }

    @Override
    public Optional<String> lockInfoJson(String name) {
        return withOpenHandle(() -> Optional.ofNullable(NativeQueue.getLockInfo(handle, name)));
    }

    @Override
    public long registerPeriodic(
            String name,
            String taskName,
            String cron,
            byte @Nullable [] args,
            @Nullable String queue,
            @Nullable String timezone,
            boolean enabled) {
        return withOpenHandle(
                () -> NativeQueue.registerPeriodic(handle, name, taskName, cron, args, queue, timezone, enabled));
    }

    @Override
    public String listPeriodicJson() {
        return withOpenHandle(() -> NativeQueue.listPeriodic(handle));
    }

    @Override
    public boolean deletePeriodic(String name) {
        return withOpenHandle(() -> NativeQueue.deletePeriodic(handle, name));
    }

    @Override
    public boolean setPeriodicEnabled(String name, boolean enabled) {
        return withOpenHandle(() -> NativeQueue.setPeriodicEnabled(handle, name, enabled));
    }

    @Override
    public void registerSubscription(
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            @Nullable String ownerWorkerIdOrNull) {
        // Route the settings-less form through the full one (nulls = queue
        // defaults) so this backend is reachable via either overload.
        registerSubscription(topic, subscriptionName, taskName, queue, durable, ownerWorkerIdOrNull, null, null, null);
    }

    @Override
    public void registerSubscription(
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            @Nullable String ownerWorkerIdOrNull,
            @Nullable Integer priority,
            @Nullable Integer maxRetries,
            @Nullable Long timeoutMs) {
        // Default to fan-out; the mode-carrying overload handles log subscriptions.
        registerSubscription(
                topic,
                subscriptionName,
                taskName,
                queue,
                durable,
                ownerWorkerIdOrNull,
                priority,
                maxRetries,
                timeoutMs,
                "fanout");
    }

    @Override
    public void registerSubscription(
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
        // JNI carries primitives, not boxed nullables — a null delivery setting
        // crosses as the MIN sentinel the native side reads as "queue default".
        int nativePriority = priority == null ? Integer.MIN_VALUE : priority;
        int nativeMaxRetries = maxRetries == null ? Integer.MIN_VALUE : maxRetries;
        long nativeTimeoutMs = timeoutMs == null ? Long.MIN_VALUE : timeoutMs;
        withOpenHandle(() -> {
            NativeQueue.registerSubscription(
                    handle,
                    topic,
                    subscriptionName,
                    taskName,
                    queue,
                    durable,
                    ownerWorkerIdOrNull,
                    nativePriority,
                    nativeMaxRetries,
                    nativeTimeoutMs,
                    mode);
            return null;
        });
    }

    @Override
    public String listSubscriptionsJson(@Nullable String topicOrNull) {
        return withOpenHandle(() -> NativeQueue.listSubscriptions(handle, topicOrNull));
    }

    @Override
    public boolean unsubscribe(String topic, String subscriptionName) {
        return withOpenHandle(() -> NativeQueue.unsubscribe(handle, topic, subscriptionName));
    }

    @Override
    public boolean setSubscriptionActive(String topic, String subscriptionName, boolean active) {
        return withOpenHandle(() -> NativeQueue.setSubscriptionActive(handle, topic, subscriptionName, active));
    }

    @Override
    public String topicBacklogStatsJson() {
        return withOpenHandle(() -> NativeQueue.topicBacklogStats(handle));
    }

    @Override
    public long reapEphemeralSubscriptions() {
        return withOpenHandle(() -> NativeQueue.reapEphemeralSubscriptions(handle));
    }

    @Override
    public String publishJson(String topic, byte[] payload, String optionsJson) {
        return withOpenHandle(() -> NativeQueue.publish(handle, topic, payload, optionsJson));
    }

    @Override
    public String readTopicMessagesJson(String topic, String subscriptionName, long limit) {
        return withOpenHandle(() -> NativeQueue.readTopicMessages(handle, topic, subscriptionName, limit));
    }

    @Override
    public boolean ackTopicCursor(String topic, String subscriptionName, String cursor) {
        return withOpenHandle(() -> NativeQueue.ackTopicCursor(handle, topic, subscriptionName, cursor));
    }

    @Override
    public String leaseTopicMessagesJson(String topic, String subscriptionName, long limit, long visibilityMs) {
        return withOpenHandle(
                () -> NativeQueue.leaseTopicMessages(handle, topic, subscriptionName, limit, visibilityMs));
    }

    @Override
    public boolean ackMessage(String topic, String subscriptionName, String messageId) {
        return withOpenHandle(() -> NativeQueue.ackMessage(handle, topic, subscriptionName, messageId));
    }

    @Override
    public boolean nackMessage(String topic, String subscriptionName, String messageId) {
        return withOpenHandle(() -> NativeQueue.nackMessage(handle, topic, subscriptionName, messageId));
    }

    @Override
    public String topicLogStatsJson() {
        return withOpenHandle(() -> NativeQueue.topicLogStats(handle));
    }

    @Override
    public void declareTopic(String name, @Nullable Long retentionMs) {
        // JNI carries a primitive long — a null retention crosses as the MIN
        // sentinel the native side reads as "unbounded".
        long nativeRetentionMs = retentionMs == null ? Long.MIN_VALUE : retentionMs;
        withOpenHandle(() -> {
            NativeQueue.declareTopic(handle, name, nativeRetentionMs);
            return null;
        });
    }

    @Override
    public String listDeclaredTopicsJson() {
        return withOpenHandle(() -> NativeQueue.listDeclaredTopics(handle));
    }

    @Override
    public String submitWorkflow(
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
        return withOpenHandle(() -> NativeWorkflows.submitWorkflow(
                handle,
                name,
                version,
                stepsJson,
                payloadNames,
                payloads,
                queueDefault,
                paramsJson,
                deferredNames,
                parentRunId,
                parentNodeName));
    }

    @Override
    public String markWorkflowNodeResult(String jobId, boolean succeeded, @Nullable String error, boolean skipCascade) {
        return withOpenHandle(
                () -> NativeWorkflows.markWorkflowNodeResult(handle, jobId, succeeded, error, skipCascade));
    }

    @Override
    public Optional<String> getWorkflowStatusJson(String runId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorkflows.getWorkflowStatus(handle, runId)));
    }

    @Override
    public String listWorkflowRunsJson(
            @Nullable String definitionNameOrNull, @Nullable String stateOrNull, long limit, long offset) {
        return withOpenHandle(
                () -> NativeWorkflows.listWorkflowRuns(handle, definitionNameOrNull, stateOrNull, limit, offset));
    }

    @Override
    public Optional<String> getWorkflowRunJson(String runId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorkflows.getWorkflowRun(handle, runId)));
    }

    @Override
    public String getWorkflowChildrenJson(String runId) {
        return withOpenHandle(() -> NativeWorkflows.getWorkflowChildren(handle, runId));
    }

    @Override
    public Optional<String> getWorkflowDagJson(String runId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorkflows.getWorkflowDag(handle, runId)));
    }

    @Override
    public void cancelWorkflowRun(String runId) {
        withOpenHandle(() -> {
            NativeWorkflows.cancelWorkflowRun(handle, runId);
            return null;
        });
    }

    @Override
    public Optional<String> getWorkflowPlanJson(String runId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorkflows.getWorkflowPlan(handle, runId)));
    }

    @Override
    public Optional<String> workflowNodeForJobJson(String jobId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorkflows.workflowNodeForJob(handle, jobId)));
    }

    @Override
    public Optional<String> workflowNameForRun(String runId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorkflows.workflowNameForRun(handle, runId)));
    }

    @Override
    public String[] expandFanOut(
            String runId,
            String parentNode,
            String[] childNames,
            byte[][] childPayloads,
            String taskName,
            String queue,
            int maxRetries,
            long timeoutMs,
            int priority) {
        return withOpenHandle(() -> NativeWorkflows.expandFanOut(
                handle,
                runId,
                parentNode,
                childNames,
                childPayloads,
                taskName,
                queue,
                maxRetries,
                timeoutMs,
                priority));
    }

    @Override
    public Optional<String> checkFanOutCompletionJson(String runId, String parentNode) {
        return withOpenHandle(
                () -> Optional.ofNullable(NativeWorkflows.checkFanOutCompletion(handle, runId, parentNode)));
    }

    @Override
    public String createDeferredJob(
            String runId,
            String nodeName,
            byte[] payload,
            String taskName,
            String queue,
            int maxRetries,
            long timeoutMs,
            int priority) {
        return withOpenHandle(() -> NativeWorkflows.createDeferredJob(
                handle, runId, nodeName, payload, taskName, queue, maxRetries, timeoutMs, priority));
    }

    @Override
    public void cascadeSkipPending(String runId) {
        withOpenHandle(() -> {
            NativeWorkflows.cascadeSkipPending(handle, runId);
            return null;
        });
    }

    @Override
    public Optional<String> finalizeRunIfTerminal(String runId) {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorkflows.finalizeRunIfTerminal(handle, runId)));
    }

    @Override
    public void setWorkflowNodeWaitingApproval(String runId, String nodeName) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowNodeWaitingApproval(handle, runId, nodeName);
            return null;
        });
    }

    @Override
    public void resolveWorkflowGate(String runId, String nodeName, boolean approved, @Nullable String error) {
        withOpenHandle(() -> {
            NativeWorkflows.resolveWorkflowGate(handle, runId, nodeName, approved, error);
            return null;
        });
    }

    @Override
    public void setWorkflowNodeRunning(String runId, String nodeName) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowNodeRunning(handle, runId, nodeName);
            return null;
        });
    }

    @Override
    public void failWorkflowNode(String runId, String nodeName, @Nullable String error) {
        withOpenHandle(() -> {
            NativeWorkflows.failWorkflowNode(handle, runId, nodeName, error);
            return null;
        });
    }

    @Override
    public void skipWorkflowNode(String runId, String nodeName) {
        withOpenHandle(() -> {
            NativeWorkflows.skipWorkflowNode(handle, runId, nodeName);
            return null;
        });
    }

    @Override
    public void setWorkflowNodeCacheHit(String runId, String nodeName) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowNodeCacheHit(handle, runId, nodeName);
            return null;
        });
    }

    @Override
    public void setWorkflowRunCompensating(String runId) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowRunCompensating(handle, runId);
            return null;
        });
    }

    @Override
    public void setWorkflowRunCompensated(String runId, long completedAt) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowRunCompensated(handle, runId, completedAt);
            return null;
        });
    }

    @Override
    public void setWorkflowRunCompensationFailed(String runId, long completedAt, @Nullable String error) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowRunCompensationFailed(handle, runId, completedAt, error);
            return null;
        });
    }

    @Override
    public void setWorkflowRunCompletedWithFailures(String runId, long completedAt) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowRunCompletedWithFailures(handle, runId, completedAt);
            return null;
        });
    }

    @Override
    public void setWorkflowNodeCompensationJob(
            String runId, String nodeName, String compensationJobId, long startedAt) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowNodeCompensationJob(handle, runId, nodeName, compensationJobId, startedAt);
            return null;
        });
    }

    @Override
    public void setWorkflowNodeCompensated(String runId, String nodeName, long completedAt) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowNodeCompensated(handle, runId, nodeName, completedAt);
            return null;
        });
    }

    @Override
    public void setWorkflowNodeCompensationFailed(
            String runId, String nodeName, @Nullable String error, long completedAt) {
        withOpenHandle(() -> {
            NativeWorkflows.setWorkflowNodeCompensationFailed(handle, runId, nodeName, error, completedAt);
            return null;
        });
    }

    @Override
    public WorkerControl startWorker(WorkerBridge bridge, String optionsJson) {
        return withOpenHandle(() -> new JniWorkerControl(NativeQueue.runWorker(handle, bridge, optionsJson)));
    }

    /** Idempotent: frees the native queue handle exactly once, after in-flight calls drain. */
    @Override
    public void close() {
        stateLock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                NativeQueue.close(handle);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }
}
