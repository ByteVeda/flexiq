package org.byteveda.flexiq.internal;

import org.jspecify.annotations.Nullable;

/**
 * JNI surface over the FlexiQ core.
 *
 * <p>Opaque job payloads cross as {@code byte[]}; options, filters, and views
 * cross as JSON strings. Methods throw {@link org.byteveda.flexiq.FlexiQException}
 * on native failure. The {@code handle} is an opaque pointer from {@link #open}.
 */
public final class NativeQueue {
    static {
        NativeLoader.load();
    }

    private NativeQueue() {}

    // ── Lifecycle ───────────────────────────────────────────────────
    /**
     * Open a queue over the configured storage.
     *
     * @param optionsJson the options as JSON
     * @return an opaque handle for every other call here
     */
    public static native long open(String optionsJson);

    /**
     * Release a queue handle. The handle is unusable afterwards.
     *
     * @param handle the queue handle from {@link #open}
     */
    public static native void close(long handle);

    // ── Producer ────────────────────────────────────────────────────
    /**
     * Enqueue one job.
     *
     * @param handle the queue handle from {@link #open}
     * @param taskName the task's registered name
     * @param payload the encoded payload, opaque to the core
     * @param optionsJson the options as JSON
     * @return the new job's id
     */
    public static native String enqueue(long handle, String taskName, byte[] payload, String optionsJson);

    /**
     * Enqueue a batch; {@code optionsJson} is a JSON array, one entry per payload.
     *
     * @param handle the queue handle from {@link #open}
     * @param taskName the task's registered name
     * @param payloads one encoded payload per job
     * @param optionsJson the options as JSON
     * @return the job ids, in input order
     */
    public static native String[] enqueueMany(long handle, String taskName, byte[][] payloads, String optionsJson);

    /**
     * Returns a JSON job view, or {@code null} if absent.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return a JSON job view, or {@code null}
     */
    public static native String getJob(long handle, String jobId);

    /**
     * Returns the job's serialized result, or {@code null} if absent/incomplete.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return the encoded result, or {@code null}
     */
    public static native byte[] getResult(long handle, String jobId);

    /**
     * Cancel a job outright.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return whether a job was cancelled
     */
    public static native boolean cancel(long handle, String jobId);

    /**
     * Ask a running job to stop cooperatively.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return whether the request was recorded
     */
    public static native boolean requestCancel(long handle, String jobId);

    /**
     * Whether a cooperative cancel has been requested.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return whether the handler should wind down
     */
    public static native boolean isCancelRequested(long handle, String jobId);

    /**
     * Record how far a running job has got.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @param progress the percentage the handler is reporting
     */
    public static native void setProgress(long handle, String jobId, int progress);

    // ── Inspection ──────────────────────────────────────────────────
    /**
     * Job counts by status across every queue.
     *
     * @param handle the queue handle from {@link #open}
     * @return the counts as JSON
     */
    public static native String stats(long handle);

    /**
     * Job counts by status for one queue.
     *
     * @param handle the queue handle from {@link #open}
     * @param queue the queue name
     * @return the counts as JSON
     */
    public static native String statsByQueue(long handle, String queue);

    /**
     * Pending count for one queue — the primitive behind {@code maxPending}.
     *
     * @param handle the queue handle from {@link #open}
     * @param queue the queue name
     * @return how many jobs are waiting
     */
    public static native long countPendingByQueue(long handle, String queue);

    /**
     * Job counts by status, one entry per queue.
     *
     * @param handle the queue handle from {@link #open}
     * @return the counts as JSON, keyed by queue
     */
    public static native String statsAllQueues(long handle);

    /**
     * A page of jobs matching a filter.
     *
     * @param handle the queue handle from {@link #open}
     * @param filterJson a {@code JobFilter} as JSON
     * @return the jobs as a JSON array
     */
    public static native String listJobs(long handle, String filterJson);

    /**
     * A keyset-paginated page of jobs.
     *
     * @param handle the queue handle from {@link #open}
     * @param filterJson a {@code JobFilter} as JSON
     * @param afterOrNull the cursor from the previous page, or {@code null} for the first
     * @return the page as JSON, carrying the next cursor
     */
    public static native String listJobsAfter(long handle, String filterJson, @Nullable String afterOrNull);

    /**
     * A keyset-paginated page of archived jobs.
     *
     * @param handle the queue handle from {@link #open}
     * @param limit the page size
     * @param afterOrNull the cursor from the previous page, or {@code null} for the first
     * @return the page as JSON, carrying the next cursor
     */
    public static native String listArchivedAfter(long handle, long limit, @Nullable String afterOrNull);

    /**
     * A job's per-attempt error history.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return the attempts as a JSON array
     */
    public static native String jobErrors(long handle, String jobId);

    /**
     * Per-execution task metrics.
     *
     * @param handle the queue handle from {@link #open}
     * @param taskNameOrNull narrow to one task, or {@code null} for every task
     * @param sinceMs a Unix-millisecond floor
     * @return the metrics as a JSON array
     */
    public static native String metrics(long handle, @Nullable String taskNameOrNull, long sinceMs);

    /**
     * Every worker in the registry.
     *
     * @param handle the queue handle from {@link #open}
     * @return the workers as a JSON array
     */
    public static native String listWorkers(long handle);

    /**
     * Every task's circuit-breaker state.
     *
     * @param handle the queue handle from {@link #open}
     * @return the states as a JSON array
     */
    public static native String listCircuitBreakers(long handle);

    /**
     * Re-enqueue a copy of a job.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return the new job's id
     */
    public static native String replayJob(long handle, String jobId);

    /**
     * Every replay minted from one job.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return the replays as a JSON array
     */
    public static native String getReplayHistory(long handle, String jobId);

    /**
     * A job's dependency graph.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return the graph as JSON
     */
    public static native String jobDag(long handle, String jobId);

    // ── Admin ───────────────────────────────────────────────────────
    /**
     * A page of the dead-letter queue.
     *
     * @param handle the queue handle from {@link #open}
     * @param limit the page size
     * @param offset how many rows to skip
     * @return the entries as a JSON array
     */
    public static native String listDead(long handle, long limit, long offset);

    /**
     * Re-enqueue a dead-letter entry as a fresh job.
     *
     * @param handle the queue handle from {@link #open}
     * @param deadId the dead-letter row's id
     * @return the new job's id
     */
    public static native String retryDead(long handle, String deadId);

    /**
     * Discard a dead-letter entry without re-enqueuing it.
     *
     * @param handle the queue handle from {@link #open}
     * @param deadId the dead-letter row's id
     * @return whether an entry was removed
     */
    public static native boolean deleteDead(long handle, String deadId);

    /**
     * Force a stuck Running job back to Pending; false when missing or not Running.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return whether the job was moved back to Pending
     */
    public static native boolean requeueJob(long handle, String jobId);

    /**
     * Delete dead-letter entries older than a cutoff.
     *
     * @param handle the queue handle from {@link #open}
     * @param olderThanMs a Unix-millisecond cutoff; rows older than it are removed
     * @return how many were removed
     */
    public static native long purgeDead(long handle, long olderThanMs);

    /**
     * Dead-letter entries for one task, as a JSON array.
     *
     * @param handle the queue handle from {@link #open}
     * @param taskName the task's registered name
     * @param limit the page size
     * @param offset how many rows to skip
     * @return the entries as a JSON array
     */
    public static native String listDeadByTask(long handle, String taskName, long limit, long offset);

    /**
     * Delete every dead-letter entry for a task; returns the number removed.
     *
     * @param handle the queue handle from {@link #open}
     * @param taskName the task's registered name
     * @return how many were removed
     */
    public static native long purgeDeadByTask(long handle, String taskName);

    /**
     * Delete completed jobs older than a cutoff.
     *
     * @param handle the queue handle from {@link #open}
     * @param olderThanMs a Unix-millisecond cutoff; rows older than it are removed
     * @return how many were removed
     */
    public static native long purgeCompleted(long handle, long olderThanMs);

    /**
     * Stop dispatching from a queue; enqueues still succeed.
     *
     * @param handle the queue handle from {@link #open}
     * @param queue the queue name
     */
    public static native void pauseQueue(long handle, String queue);

    /**
     * Resume dispatching from a paused queue.
     *
     * @param handle the queue handle from {@link #open}
     * @param queue the queue name
     */
    public static native void resumeQueue(long handle, String queue);

    /**
     * Which queues are currently paused.
     *
     * @param handle the queue handle from {@link #open}
     * @return the names as a JSON array
     */
    public static native String listPausedQueues(long handle);

    /**
     * Returns the value, or {@code null} if unset.
     *
     * @param handle the queue handle from {@link #open}
     * @param key the settings document's key
     * @return the value, or {@code null}
     */
    public static native String getSetting(long handle, String key);

    /**
     * Write a settings document, overwriting whatever was there.
     *
     * @param handle the queue handle from {@link #open}
     * @param key the settings document's key
     * @param value the content to store
     */
    public static native void setSetting(long handle, String key, String value);

    /**
     * Writes only if the key still holds {@code expectedOrNull}, where
     * {@code null} means it must be unset.
     *
     * @param handle the queue handle from {@link #open}
     * @param key the settings document's key
     * @param expectedOrNull the content the write is conditional on; {@code null} means the key must be unset
     * @param value the content to store
     * @return false when another writer got there first.
     */
    public static native boolean setSettingIf(long handle, String key, @Nullable String expectedOrNull, String value);

    /**
     * Remove a settings document.
     *
     * @param handle the queue handle from {@link #open}
     * @param key the settings document's key
     * @return whether a row existed
     */
    public static native boolean deleteSetting(long handle, String key);

    /**
     * Every settings document.
     *
     * @param handle the queue handle from {@link #open}
     * @return the documents as a JSON object
     */
    public static native String listSettings(long handle);

    /**
     * Applies pending schema changes; returns the report as JSON.
     *
     * @param handle the queue handle from {@link #open}
     * @return the migration report as JSON
     */
    public static native String migrate(long handle);

    /**
     * The lowest contract level a process may speak and still open this storage.
     *
     * @param handle the queue handle from {@link #open}
     * @return the contract level this storage refuses to open below
     */
    public static native int minContract(long handle);

    /**
     * Raises or lowers that floor; a level this build cannot speak is rejected.
     *
     * @param handle the queue handle from {@link #open}
     * @param level the severity's wire form
     */
    public static native void setMinContract(long handle, int level);

    /**
     * Settings-key prefixes the dashboard's generic KV surface must hide.
     *
     * @return the prefixes the generic settings routes must hide
     */
    public static native String[] reservedSettingPrefixes();

    /**
     * The published retention windows as JSON, or {@code null} if unreported.
     *
     * @param handle the queue handle from {@link #open}
     * @return the windows as JSON, or {@code null}
     */
    public static native String effectiveRetention(long handle);

    /**
     * Preview counts as JSON. {@code retentionJson} is a candidate retention
     * spec (camelCase seconds) to preview those windows; {@code null} previews
     * the policy the elected cleaner reported for this namespace, falling back
     * to the recommended defaults only when no cleaner has swept yet.
     *
     * @param handle the queue handle from {@link #open}
     * @param retentionJson a candidate retention spec as camelCase seconds, or {@code null}
     * @return the counts a purge would delete, as JSON
     */
    public static native String dryRunRetention(long handle, @Nullable String retentionJson);

    // ── Logs ────────────────────────────────────────────────────────
    /**
     * Append one structured log line for a job.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param level the severity's wire form
     * @param message the line itself
     * @param extraOrNull structured context as JSON, or {@code null}
     */
    public static native void writeTaskLog(
            long handle, String jobId, String taskName, String level, String message, @Nullable String extraOrNull);

    /**
     * Every log line one job emitted.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @return the lines as a JSON array
     */
    public static native String getTaskLogs(long handle, String jobId);

    /**
     * A job's log lines after a cursor id.
     *
     * @param handle the queue handle from {@link #open}
     * @param jobId the job's id
     * @param afterIdOrNull the last id already read, or {@code null} to start at the beginning
     * @return the lines as a JSON array
     */
    public static native String getTaskLogsAfter(long handle, String jobId, @Nullable String afterIdOrNull);

    /**
     * Logs across jobs, filtered by task, level and time.
     *
     * @param handle the queue handle from {@link #open}
     * @param taskNameOrNull narrow to one task, or {@code null} for every task
     * @param levelOrNull narrow to one severity, or {@code null} for every severity
     * @param sinceMs a Unix-millisecond floor
     * @param limit the page size
     * @return the lines as a JSON array
     */
    public static native String queryTaskLogs(
            long handle, @Nullable String taskNameOrNull, @Nullable String levelOrNull, long sinceMs, long limit);

    // ── Locks ───────────────────────────────────────────────────────
    /**
     * Take a TTL-bounded advisory lock.
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @param ownerId the lock holder's per-instance id
     * @param ttlMs the lifetime in milliseconds, measured from now
     * @return whether the lock was taken
     */
    public static native boolean acquireLock(long handle, String name, String ownerId, long ttlMs);

    /**
     * Give a lock up, if this owner still holds it.
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @param ownerId the lock holder's per-instance id
     * @return whether it was released
     */
    public static native boolean releaseLock(long handle, String name, String ownerId);

    /**
     * Push a held lock's expiry out.
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @param ownerId the lock holder's per-instance id
     * @param ttlMs the lifetime in milliseconds, measured from now
     * @return whether it was still held and moved
     */
    public static native boolean extendLock(long handle, String name, String ownerId, long ttlMs);

    /**
     * Returns JSON holder info, or {@code null} if free.
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @return JSON holder info, or {@code null}
     */
    public static native String getLockInfo(long handle, String name);

    // ── Periodic ────────────────────────────────────────────────────
    /**
     * Register (or replace) a cron task; returns the next fire time (Unix ms).
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @param taskName the task's registered name
     * @param cron the expression deciding when it fires
     * @param args the encoded payload each firing enqueues, or {@code null} for none
     * @param queue the queue name
     * @param timezone the IANA zone the cron is read in, or {@code null} for UTC
     * @param enabled {@code false} to keep the registration but stop it firing
     * @return the next fire time, in Unix milliseconds
     */
    public static native long registerPeriodic(
            long handle,
            String name,
            String taskName,
            String cron,
            byte @Nullable [] args,
            @Nullable String queue,
            @Nullable String timezone,
            boolean enabled);

    /**
     * A JSON array of every registered periodic task (enabled and paused).
     *
     * @param handle the queue handle from {@link #open}
     * @return the schedules as a JSON array
     */
    public static native String listPeriodic(long handle);

    /**
     * Remove a periodic task; false if none had that name.
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @return whether a schedule was removed
     */
    public static native boolean deletePeriodic(long handle, String name);

    /**
     * Pause (false) or resume (true) a periodic task; false if none had that name.
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @param enabled {@code false} to keep the registration but stop it firing
     * @return whether a schedule was changed
     */
    public static native boolean setPeriodicEnabled(long handle, String name, boolean enabled);

    // ── Pub/Sub ─────────────────────────────────────────────────────
    /**
     * Insert or update a topic subscription (idempotent on topic + name). The
     * subscriber task's delivery settings persist on the row; {@code priority}/
     * {@code maxRetries} of {@link Integer#MIN_VALUE} and {@code timeoutMs} of
     * {@link Long#MIN_VALUE} mean "unset — take the queue default". {@code mode} is
     * {@code "fanout"} (one job per publish) or {@code "log"} (append-once + cursor).
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param taskName the task's registered name
     * @param queue the queue name
     * @param durable {@code false} ties the registration to one worker process
     * @param ownerWorkerIdOrNull the owning worker, required for an ephemeral subscription
     * @param priority the dispatch priority, or {@link Integer#MIN_VALUE} for the queue default
     * @param maxRetries the retry ceiling, or {@link Integer#MIN_VALUE} for the queue default
     * @param timeoutMs the per-attempt timeout, or {@link Long#MIN_VALUE} for the queue default
     * @param mode {@code "fanout"} or {@code "log"}
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

    /**
     * A JSON array of subscriptions — all of them, or only a topic's active ones.
     *
     * @param handle the queue handle from {@link #open}
     * @param topicOrNull narrow to one topic's active subscriptions, or {@code null} for every subscription
     * @return the subscriptions as a JSON array
     */
    public static native String listSubscriptions(long handle, @Nullable String topicOrNull);

    /**
     * Remove a subscription; false if none matched.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @return whether a subscription was removed
     */
    public static native boolean unsubscribe(long handle, String topic, String subscriptionName);

    /**
     * Pause (false) or resume (true) a subscription; false if none matched.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param active {@code false} to stop deliveries without unsubscribing
     * @return whether a subscription was changed
     */
    public static native boolean setSubscriptionActive(
            long handle, String topic, String subscriptionName, boolean active);

    /**
     * A JSON array of per-subscription backlog snapshots, one per registered subscription.
     *
     * @param handle the queue handle from {@link #open}
     * @return the snapshots as a JSON array
     */
    public static native String topicBacklogStats(long handle);

    /**
     * Drop ephemeral subscriptions whose owning worker is gone; returns the count removed.
     *
     * @param handle the queue handle from {@link #open}
     * @return how many were removed
     */
    public static native long reapEphemeralSubscriptions(long handle);

    /**
     * Publish to a topic; returns the created delivery jobs as a JSON array.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param payload the encoded payload, opaque to the core
     * @param optionsJson the options as JSON
     * @return the created delivery jobs as a JSON array
     */
    public static native String publish(long handle, String topic, byte[] payload, String optionsJson);

    /**
     * Pull messages after a log subscription's cursor, as a JSON array of message views.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param limit the page size
     * @return the messages as a JSON array, oldest first
     */
    public static native String readTopicMessages(long handle, String topic, String subscriptionName, long limit);

    /**
     * Advance a log subscription's cursor (monotonic); false if nothing moved.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param cursor the id of the last message handled
     * @return whether the cursor moved
     */
    public static native boolean ackTopicCursor(long handle, String topic, String subscriptionName, String cursor);

    /**
     * Lease up to {@code limit} available messages for {@code visibilityMs}, as a JSON array of message views.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param limit the page size
     * @param visibilityMs how long each lease holds before the message redelivers
     * @return the leased messages as a JSON array
     */
    public static native String leaseTopicMessages(
            long handle, String topic, String subscriptionName, long limit, long visibilityMs);

    /**
     * Ack one leased message; false if there was no un-acked delivery to ack.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param messageId the leased message's id
     * @return whether a delivery was acked
     */
    public static native boolean ackMessage(long handle, String topic, String subscriptionName, String messageId);

    /**
     * Nack one leased message; false if there was no un-acked delivery to nack.
     *
     * @param handle the queue handle from {@link #open}
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param messageId the leased message's id
     * @return whether a delivery was nacked
     */
    public static native boolean nackMessage(long handle, String topic, String subscriptionName, String messageId);

    /**
     * A JSON array of per-log-subscription lag snapshots.
     *
     * @param handle the queue handle from {@link #open}
     * @return the snapshots as a JSON array
     */
    public static native String topicLogStats(long handle);

    /**
     * Declare a log topic (idempotent) so its publishes are retained even with no
     * subscriber. {@code retentionMs} of {@link Long#MIN_VALUE} means "unbounded —
     * keep until consumed"; mode is always {@code "log"}.
     *
     * @param handle the queue handle from {@link #open}
     * @param name the record's name
     * @param retentionMs how long a sub-less backlog is kept, or {@link Long#MIN_VALUE} to keep until consumed
     */
    public static native void declareTopic(long handle, String name, long retentionMs);

    /**
     * A JSON array of declared topics.
     *
     * @param handle the queue handle from {@link #open}
     * @return the topics as a JSON array
     */
    public static native String listDeclaredTopics(long handle);

    /**
     * Whether this backend has a durable-step store.
     *
     * @param handle the queue handle from {@link #open}
     * @return whether {@code job_steps} is available
     */
    public static native boolean supportsSteps(long handle);

    // ── Worker ──────────────────────────────────────────────────────
    /**
     * Start a worker; returns its handle. {@code bridge} is a {@code WorkerBridge}.
     *
     * @param handle the queue handle from {@link #open}
     * @param bridge the {@code WorkerBridge} each claimed job is handed to
     * @param optionsJson the options as JSON
     * @return the worker handle
     */
    public static native long runWorker(long handle, Object bridge, String optionsJson);
}
