package org.byteveda.taskito.internal;

import org.jspecify.annotations.Nullable;

/**
 * JNI surface over the Taskito core.
 *
 * <p>Opaque job payloads cross as {@code byte[]}; options, filters, and views
 * cross as JSON strings. Methods throw {@link org.byteveda.taskito.TaskitoException}
 * on native failure. The {@code handle} is an opaque pointer from {@link #open}.
 */
public final class NativeQueue {
    static {
        NativeLoader.load();
    }

    private NativeQueue() {}

    // ── Lifecycle ───────────────────────────────────────────────────
    public static native long open(String optionsJson);

    public static native void close(long handle);

    // ── Producer ────────────────────────────────────────────────────
    public static native String enqueue(long handle, String taskName, byte[] payload, String optionsJson);

    public static native String[] enqueueMany(long handle, String taskName, byte[][] payloads, String optionsJson);

    /** Returns a JSON job view, or {@code null} if absent. */
    public static native String getJob(long handle, String jobId);

    /** Returns the job's serialized result, or {@code null} if absent/incomplete. */
    public static native byte[] getResult(long handle, String jobId);

    public static native boolean cancel(long handle, String jobId);

    public static native boolean requestCancel(long handle, String jobId);

    public static native boolean isCancelRequested(long handle, String jobId);

    public static native void setProgress(long handle, String jobId, int progress);

    // ── Inspection ──────────────────────────────────────────────────
    public static native String stats(long handle);

    public static native String statsByQueue(long handle, String queue);

    public static native long countPendingByQueue(long handle, String queue);

    public static native String statsAllQueues(long handle);

    public static native String listJobs(long handle, String filterJson);

    public static native String listJobsAfter(long handle, String filterJson, @Nullable String afterOrNull);

    public static native String listArchivedAfter(long handle, long limit, @Nullable String afterOrNull);

    public static native String jobErrors(long handle, String jobId);

    public static native String metrics(long handle, @Nullable String taskNameOrNull, long sinceMs);

    public static native String listWorkers(long handle);

    public static native String listCircuitBreakers(long handle);

    public static native String replayJob(long handle, String jobId);

    public static native String getReplayHistory(long handle, String jobId);

    public static native String jobDag(long handle, String jobId);

    // ── Admin ───────────────────────────────────────────────────────
    public static native String listDead(long handle, long limit, long offset);

    public static native String retryDead(long handle, String deadId);

    public static native boolean deleteDead(long handle, String deadId);

    /** Force a stuck Running job back to Pending; false when missing or not Running. */
    public static native boolean requeueJob(long handle, String jobId);

    public static native long purgeDead(long handle, long olderThanMs);

    /** Dead-letter entries for one task, as a JSON array. */
    public static native String listDeadByTask(long handle, String taskName, long limit, long offset);

    /** Delete every dead-letter entry for a task; returns the number removed. */
    public static native long purgeDeadByTask(long handle, String taskName);

    public static native long purgeCompleted(long handle, long olderThanMs);

    public static native void pauseQueue(long handle, String queue);

    public static native void resumeQueue(long handle, String queue);

    public static native String listPausedQueues(long handle);

    /** Returns the value, or {@code null} if unset. */
    public static native String getSetting(long handle, String key);

    public static native void setSetting(long handle, String key, String value);

    public static native boolean deleteSetting(long handle, String key);

    public static native String listSettings(long handle);

    /** Settings-key prefixes the dashboard's generic KV surface must hide. */
    public static native String[] reservedSettingPrefixes();

    /** The published retention windows as JSON, or {@code null} if unreported. */
    public static native String effectiveRetention(long handle);

    /**
     * Preview counts as JSON. {@code retentionJson} is a candidate retention
     * spec (camelCase seconds) to preview those windows; {@code null} previews
     * the policy the elected cleaner reported for this namespace, falling back
     * to the recommended defaults only when no cleaner has swept yet.
     */
    public static native String dryRunRetention(long handle, @Nullable String retentionJson);

    // ── Logs ────────────────────────────────────────────────────────
    public static native void writeTaskLog(
            long handle, String jobId, String taskName, String level, String message, @Nullable String extraOrNull);

    public static native String getTaskLogs(long handle, String jobId);

    public static native String getTaskLogsAfter(long handle, String jobId, @Nullable String afterIdOrNull);

    public static native String queryTaskLogs(
            long handle, @Nullable String taskNameOrNull, @Nullable String levelOrNull, long sinceMs, long limit);

    // ── Locks ───────────────────────────────────────────────────────
    public static native boolean acquireLock(long handle, String name, String ownerId, long ttlMs);

    public static native boolean releaseLock(long handle, String name, String ownerId);

    public static native boolean extendLock(long handle, String name, String ownerId, long ttlMs);

    /** Returns JSON holder info, or {@code null} if free. */
    public static native String getLockInfo(long handle, String name);

    // ── Periodic ────────────────────────────────────────────────────
    /** Register (or replace) a cron task; returns the next fire time (Unix ms). */
    public static native long registerPeriodic(
            long handle,
            String name,
            String taskName,
            String cron,
            byte @Nullable [] args,
            @Nullable String queue,
            @Nullable String timezone,
            boolean enabled);

    /** A JSON array of every registered periodic task (enabled and paused). */
    public static native String listPeriodic(long handle);

    /** Remove a periodic task; false if none had that name. */
    public static native boolean deletePeriodic(long handle, String name);

    /** Pause (false) or resume (true) a periodic task; false if none had that name. */
    public static native boolean setPeriodicEnabled(long handle, String name, boolean enabled);

    // ── Pub/Sub ─────────────────────────────────────────────────────
    /**
     * Insert or update a topic subscription (idempotent on topic + name). The
     * subscriber task's delivery settings persist on the row; {@code priority}/
     * {@code maxRetries} of {@link Integer#MIN_VALUE} and {@code timeoutMs} of
     * {@link Long#MIN_VALUE} mean "unset — take the queue default". {@code mode} is
     * {@code "fanout"} (one job per publish) or {@code "log"} (append-once + cursor).
     */
    public static native void registerSubscription(
            long handle,
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            @Nullable String ownerWorkerIdOrNull,
            int priority,
            int maxRetries,
            long timeoutMs,
            String mode);

    /** A JSON array of subscriptions — all of them, or only a topic's active ones. */
    public static native String listSubscriptions(long handle, @Nullable String topicOrNull);

    /** Remove a subscription; false if none matched. */
    public static native boolean unsubscribe(long handle, String topic, String subscriptionName);

    /** Pause (false) or resume (true) a subscription; false if none matched. */
    public static native boolean setSubscriptionActive(
            long handle, String topic, String subscriptionName, boolean active);

    /** A JSON array of per-subscription backlog snapshots, one per registered subscription. */
    public static native String topicBacklogStats(long handle);

    /** Drop ephemeral subscriptions whose owning worker is gone; returns the count removed. */
    public static native long reapEphemeralSubscriptions(long handle);

    /** Publish to a topic; returns the created delivery jobs as a JSON array. */
    public static native String publish(long handle, String topic, byte[] payload, String optionsJson);

    /** Pull messages after a log subscription's cursor, as a JSON array of message views. */
    public static native String readTopicMessages(long handle, String topic, String subscriptionName, long limit);

    /** Advance a log subscription's cursor (monotonic); false if nothing moved. */
    public static native boolean ackTopicCursor(long handle, String topic, String subscriptionName, String cursor);

    /** Lease up to {@code limit} available messages for {@code visibilityMs}, as a JSON array of message views. */
    public static native String leaseTopicMessages(
            long handle, String topic, String subscriptionName, long limit, long visibilityMs);

    /** Ack one leased message; false if there was no un-acked delivery to ack. */
    public static native boolean ackMessage(long handle, String topic, String subscriptionName, String messageId);

    /** Nack one leased message; false if there was no un-acked delivery to nack. */
    public static native boolean nackMessage(long handle, String topic, String subscriptionName, String messageId);

    /** A JSON array of per-log-subscription lag snapshots. */
    public static native String topicLogStats(long handle);

    /**
     * Declare a log topic (idempotent) so its publishes are retained even with no
     * subscriber. {@code retentionMs} of {@link Long#MIN_VALUE} means "unbounded —
     * keep until consumed"; mode is always {@code "log"}.
     */
    public static native void declareTopic(long handle, String name, long retentionMs);

    /** A JSON array of declared topics. */
    public static native String listDeclaredTopics(long handle);

    // ── Worker ──────────────────────────────────────────────────────
    /** Start a worker; returns its handle. {@code bridge} is a {@code WorkerBridge}. */
    public static native long runWorker(long handle, Object bridge, String optionsJson);
}
