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

    /**
     * Enqueue one job; {@code optionsJson} is a single {@code EnqueueOptions} object.
     *
     * @param taskName the task's registered name
     * @param payload the encoded payload, opaque to the core
     * @param optionsJson one {@code EnqueueOptions} object as JSON
     * @return the new job's id
     */
    String enqueue(String taskName, byte[] payload, String optionsJson);

    /**
     * Enqueue a batch. Unlike {@link #enqueue}, {@code optionsJson} is a JSON
     * <em>array</em> of per-job {@code EnqueueOptions}, the same length as
     * {@code payloads}. Returns the job ids in input order.
     *
     * @param taskName the task's registered name
     * @param payloads one encoded payload per job
     * @param optionsJson a JSON array of per-job options, the same length as {@code payloads}
     * @return the job ids, in input order
     */
    String[] enqueueMany(String taskName, byte[][] payloads, String optionsJson);

    /**
     * One job's view as JSON.
     *
     * @param jobId the job's id
     * @return the JSON view, or empty when no such job exists
     */
    Optional<String> getJobJson(String jobId);

    /**
     * A completed job's stored result.
     *
     * @param jobId the job's id
     * @return the encoded result, or empty when the job is absent or has not completed
     */
    Optional<byte[]> getResult(String jobId);

    /**
     * Cancel a job outright, so it never runs again.
     *
     * @param jobId the job's id
     * @return whether a job was cancelled
     */
    boolean cancel(String jobId);

    /**
     * Ask a running job to stop, for a handler that polls {@link #isCancelRequested}.
     *
     * @param jobId the job's id
     * @return whether the request was recorded
     */
    boolean requestCancel(String jobId);

    /**
     * Whether a cooperative cancel has been requested for this job.
     *
     * @param jobId the job's id
     * @return whether the handler should wind down
     */
    boolean isCancelRequested(String jobId);

    /**
     * Record how far a running job has got.
     *
     * @param jobId the job's id
     * @param progress the percentage the handler is reporting
     */
    void setProgress(String jobId, int progress);

    // ── Inspection ──────────────────────────────────────────────────
    /**
     * Job counts by status across every queue.
     *
     * @return the counts as JSON
     */
    String statsJson();

    /**
     * Job counts by status for one queue.
     *
     * @param queue the queue name
     * @return the counts as JSON
     */
    String statsByQueueJson(String queue);

    /**
     * Count pending jobs on {@code queue} — the primitive behind the
     * {@code maxPending} cap. Defaults to unsupported so this optional capability
     * doesn't break existing third-party {@code QueueBackend} implementations at
     * compile time; it is only invoked when a queue actually has a cap set.
     *
     * @param queue the queue name
     * @return how many jobs are waiting to run on it
     */
    default long countPendingByQueue(String queue) {
        throw new UnsupportedOperationException("countPendingByQueue not supported by this backend");
    }

    /**
     * Job counts by status, one entry per queue.
     *
     * @return the counts as JSON, keyed by queue name
     */
    String statsAllQueuesJson();

    /**
     * A page of jobs matching a filter.
     *
     * @param filterJson a {@code JobFilter} as JSON
     * @return the matching jobs as a JSON array
     */
    String listJobsJson(String filterJson);

    /**
     * A keyset-paginated page of jobs as JSON. Defaults to unsupported: a backend
     * without a seekable ordering cannot honour the cursor contract, and silently
     * returning an unpaginated list would look like the last page.
     *
     * @param filterJson a {@code JobFilter} as JSON
     * @param afterOrNull the cursor from the previous page, or {@code null} for the first
     * @return the page as JSON, carrying the cursor for the next one
     */
    default String listJobsAfterJson(String filterJson, @Nullable String afterOrNull) {
        throw new UnsupportedOperationException("keyset pagination not supported by this backend");
    }

    /**
     * A keyset-paginated page of archived jobs as JSON. See {@link #listJobsAfterJson}.
     *
     * @param limit the page size
     * @param afterOrNull the cursor from the previous page, or {@code null} for the first
     * @return the page as JSON, carrying the cursor for the next one
     */
    default String listArchivedAfterJson(long limit, @Nullable String afterOrNull) {
        throw new UnsupportedOperationException("keyset pagination not supported by this backend");
    }

    /**
     * A job's per-attempt error history.
     *
     * @param jobId the job's id
     * @return the attempts as a JSON array, oldest first
     */
    String jobErrorsJson(String jobId);

    /**
     * Per-execution task metrics.
     *
     * @param taskNameOrNull narrow to one task, or {@code null} for every task
     * @param sinceMs a Unix-millisecond floor on {@code recordedAt}
     * @return the metrics as a JSON array
     */
    String metricsJson(@Nullable String taskNameOrNull, long sinceMs);

    /**
     * Every worker in the registry.
     *
     * @return the workers as a JSON array; staleness is judged from each heartbeat
     */
    String listWorkersJson();

    /**
     * Circuit-breaker states as a JSON array; defaults to none for backends without breakers.
     *
     * @return the breaker states as a JSON array
     */
    default String listCircuitBreakersJson() {
        return "[]";
    }

    // ── Admin ───────────────────────────────────────────────────────
    /**
     * A page of the dead-letter queue.
     *
     * @param limit the page size
     * @param offset how many entries to skip
     * @return the entries as a JSON array
     */
    String listDeadJson(long limit, long offset);

    /**
     * Re-enqueue a dead-letter entry as a fresh job.
     *
     * @param deadId the dead-letter row's id
     * @return the new job's id
     */
    String retryDead(String deadId);

    /**
     * Re-enqueue a copy of a job and return the new id.
     *
     * @param jobId the job's id
     * @return the new job's id
     */
    default String replayJob(String jobId) {
        throw new UnsupportedOperationException("replay not supported by this backend");
    }

    /**
     * A job's replay history; defaults to none.
     *
     * @param jobId the job's id
     * @return the replays as a JSON array
     */
    default String getReplayHistoryJson(String jobId) {
        return "[]";
    }

    /**
     * A job's dependency DAG; defaults to just the empty graph.
     *
     * @param jobId the job's id
     * @return the graph as JSON: {@code nodes} and {@code edges}
     */
    default String jobDagJson(String jobId) {
        return "{\"nodes\":[],\"edges\":[]}";
    }

    /**
     * Discard a dead-letter entry without re-enqueuing it.
     *
     * @param deadId the dead-letter row's id
     * @return whether an entry was removed
     */
    boolean deleteDead(String deadId);

    /**
     * Force a stuck running job back to pending, releasing its execution claim.
     * Returns false when the job is missing or is not running.
     *
     * @param jobId the job's id
     * @return whether the job was moved back to pending
     */
    default boolean requeueJob(String jobId) {
        throw new UnsupportedOperationException("requeue is not supported by this backend");
    }

    /**
     * Delete dead-letter entries older than a cutoff.
     *
     * @param olderThanMs a Unix-millisecond cutoff; entries failed before it are removed
     * @return how many entries were removed
     */
    long purgeDead(long olderThanMs);

    /**
     * A JSON array of dead-letter entries for one task.
     *
     * @param taskName the task's registered name
     * @param limit the page size
     * @param offset how many entries to skip
     * @return the entries as a JSON array
     */
    default String listDeadByTaskJson(String taskName, long limit, long offset) {
        throw new UnsupportedOperationException("per-task dead-letter queries not supported by this backend");
    }

    /**
     * Delete every dead-letter entry for a task; returns the number removed.
     *
     * @param taskName the task's registered name
     * @return how many entries were removed
     */
    default long purgeDeadByTask(String taskName) {
        throw new UnsupportedOperationException("per-task dead-letter queries not supported by this backend");
    }

    /**
     * Delete completed jobs older than a cutoff.
     *
     * @param olderThanMs a Unix-millisecond cutoff; jobs completed before it are removed
     * @return how many jobs were removed
     */
    long purgeCompleted(long olderThanMs);

    /**
     * Stop dispatching from a queue; enqueues still succeed.
     *
     * @param queue the queue name
     */
    void pauseQueue(String queue);

    /**
     * Resume dispatching from a paused queue.
     *
     * @param queue the queue name
     */
    void resumeQueue(String queue);

    /**
     * Which queues are currently paused.
     *
     * @return the queue names as a JSON array
     */
    String listPausedQueuesJson();

    /**
     * Write a settings document, overwriting whatever was there.
     *
     * @param key the document's key
     * @param value its content
     */
    void setSetting(String key, String value);

    /**
     * {@inheritDoc}
     *
     * <p>Defaults to a <b>non-atomic</b> read-compare-write over
     * {@link #getSetting} and {@link #setSetting}, so a backend written before
     * this method existed keeps compiling and behaves exactly as it did — a
     * concurrent writer between the read and the write is still lost. Override
     * it with whatever your storage can do atomically.
     *
     * @param key the document's key
     * @param expected the content the write is conditional on; empty means "must not exist"
     * @param value the content to store
     * @return whether the write happened
     */
    @Override
    default boolean setSettingIf(String key, Optional<String> expected, String value) {
        if (!getSetting(key).equals(expected)) {
            return false;
        }
        setSetting(key, value);
        return true;
    }

    /**
     * Remove a settings document.
     *
     * @param key the document's key
     * @return whether a row existed
     */
    boolean deleteSetting(String key);

    /**
     * Every settings document.
     *
     * @return the documents as a JSON object, keyed by key
     */
    String listSettingsJson();

    /**
     * Applies pending schema changes, returning the report as JSON. Only the
     * native backend owns a schema, so only it can migrate one.
     *
     * @return the migration report as JSON
     */
    default String migrateJson() {
        throw new UnsupportedOperationException("migrate requires the native backend");
    }

    /**
     * The lowest contract level a process may speak and still open this
     * storage. The level itself is a property of the native build, so only the
     * native backend can answer.
     *
     * @return the contract level this storage refuses to open below
     */
    default int minContract() {
        throw new UnsupportedOperationException("the contract floor requires the native backend");
    }

    /**
     * Raises or lowers that floor. Native-only, for the same reason.
     *
     * @param level the new floor; a process speaking below it can no longer join
     */
    default void setMinContract(int level) {
        throw new UnsupportedOperationException("the contract floor requires the native backend");
    }

    /**
     * The retention windows the elected cleaner last published for this queue's
     * namespace, as JSON, or empty when no worker has swept yet. Defaults to
     * empty for backends that do not report one.
     *
     * @return the windows as JSON, or empty when no worker has published any
     */
    default Optional<String> effectiveRetentionJson() {
        return Optional.empty();
    }

    /**
     * Preview counts as JSON for the given candidate retention spec (camelCase
     * seconds), or {@code null} for the recommended defaults. Only the native
     * backend can count against live storage, so non-native backends do not
     * support the dry-run.
     *
     * @param retentionJson the candidate windows as camelCase seconds, or {@code null} for the recommended defaults
     * @return the counts a purge would delete, as JSON
     */
    default String dryRunRetentionJson(@Nullable String retentionJson) {
        throw new UnsupportedOperationException("retention dry-run requires the native backend");
    }

    // ── Logs ────────────────────────────────────────────────────────
    /**
     * Append one structured log line for a job.
     *
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param level the severity's wire form
     * @param message the line itself
     * @param extraOrNull structured context as JSON, or {@code null}
     */
    void writeTaskLog(String jobId, String taskName, String level, String message, @Nullable String extraOrNull);

    /**
     * Every log line one job emitted.
     *
     * @param jobId the job's id
     * @return the lines as a JSON array, in emission order
     */
    String getTaskLogsJson(String jobId);

    /**
     * Logs for a job after a cursor id; defaults to none for backends without cursor support.
     *
     * @param jobId the job's id
     * @param afterIdOrNull the last id already read, or {@code null} to start at the beginning
     * @return the lines after that cursor as a JSON array
     */
    default String getTaskLogsAfterJson(String jobId, @Nullable String afterIdOrNull) {
        return "[]";
    }

    /**
     * Logs across jobs filtered by task/level/since; defaults to none.
     *
     * @param taskNameOrNull narrow to one task, or {@code null} for every task
     * @param levelOrNull narrow to one severity, or {@code null} for every severity
     * @param sinceMs a Unix-millisecond floor on {@code loggedAt}
     * @param limit the most lines to return
     * @return the matching lines as a JSON array
     */
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
     *
     * @return whether {@code job_steps} is available; {@code false} by default
     */
    default boolean supportsSteps() {
        return false;
    }

    // ── Locks ───────────────────────────────────────────────────────
    // Optional capability: default to throwing so existing custom backends keep
    // compiling and fail explicitly only when locks are actually used.
    /**
     * Take a TTL-bounded advisory lock.
     *
     * @param name the lock's name
     * @param ownerId the holder's per-instance id; a release or extend is scoped to it
     * @param ttlMs how long it survives without an extend
     * @return whether the lock was taken
     */
    default boolean acquireLock(String name, String ownerId, long ttlMs) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    /**
     * Give a lock up, if this owner still holds it.
     *
     * @param name the lock's name
     * @param ownerId the holder's per-instance id; a release or extend is scoped to it
     * @return whether the lock was released
     */
    default boolean releaseLock(String name, String ownerId) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    /**
     * Push a held lock's expiry out.
     *
     * @param name the lock's name
     * @param ownerId the holder's per-instance id; a release or extend is scoped to it
     * @param ttlMs the new lifetime, measured from now
     * @return whether the lock was still held and its TTL moved
     */
    default boolean extendLock(String name, String ownerId, long ttlMs) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    /**
     * Who holds a lock right now.
     *
     * @param name the lock's name
     * @return the holder as JSON, or empty when the lock is free
     */
    default Optional<String> lockInfoJson(String name) {
        throw new UnsupportedOperationException("locks not supported by this backend");
    }

    // ── Periodic ────────────────────────────────────────────────────
    /**
     * Insert or update a cron schedule (idempotent on its name).
     *
     * @param name the schedule's own identity
     * @param taskName the task's registered name
     * @param cron the expression deciding when it fires
     * @param args the encoded payload each firing enqueues, or {@code null} for none
     * @param queue the queue the jobs go to, or {@code null} for the default
     * @param timezone the IANA zone the cron is read in, or {@code null} for UTC
     * @param enabled {@code false} to register it without firing
     * @return the next fire time, in Unix milliseconds
     */
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

    /**
     * A JSON array of every registered periodic task.
     *
     * @return the schedules as a JSON array
     */
    default String listPeriodicJson() {
        throw new UnsupportedOperationException("periodic tasks not supported by this backend");
    }

    /**
     * Remove a periodic task; false if none had that name.
     *
     * @param name the schedule's identity
     * @return whether a schedule was removed
     */
    default boolean deletePeriodic(String name) {
        throw new UnsupportedOperationException("periodic tasks not supported by this backend");
    }

    /**
     * Pause (false) or resume (true) a periodic task; false if none had that name.
     *
     * @param name the schedule's identity
     * @param enabled {@code false} to stop it firing
     * @return whether a schedule was changed
     */
    default boolean setPeriodicEnabled(String name, boolean enabled) {
        throw new UnsupportedOperationException("periodic tasks not supported by this backend");
    }

    // ── Pub/Sub ─────────────────────────────────────────────────────
    // Optional capability: default to throwing so existing custom backends keep
    // compiling and fail explicitly only when pub/sub is actually used.
    /** Message every pub/sub default throws with, so the refusal reads the same everywhere. */
    String PUBSUB_UNSUPPORTED = "pub/sub not supported by this backend";

    /**
     * Insert or update a topic subscription with no per-subscriber delivery
     * settings; deliveries take the queue defaults. This is the base method a
     * backend overrides — the {@code Integer/Integer/Long} overload delegates
     * here so a backend that only implements this form keeps working.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param taskName the task's registered name
     * @param queue the queue name
     * @param durable {@code false} ties the registration to one worker process
     * @param ownerWorkerIdOrNull the owning worker, required for an ephemeral subscription
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
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param taskName the task's registered name
     * @param queue the queue name
     * @param durable {@code false} ties the registration to one worker process
     * @param ownerWorkerIdOrNull the owning worker, required for an ephemeral subscription
     * @param priority the subscriber task's own default priority, or {@code null} for the queue default
     * @param maxRetries the subscriber task's own default retry budget, or {@code null}
     * @param timeoutMs the subscriber task's own default timeout, or {@code null}
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
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param taskName the task's registered name
     * @param queue the queue name
     * @param durable {@code false} ties the registration to one worker process
     * @param ownerWorkerIdOrNull the owning worker, required for an ephemeral subscription
     * @param priority the subscriber task's own default priority, or {@code null} for the queue default
     * @param maxRetries the subscriber task's own default retry budget, or {@code null}
     * @param timeoutMs the subscriber task's own default timeout, or {@code null}
     * @param mode {@code "fanout"} or {@code "log"}
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

    /**
     * A JSON array of subscriptions — all of them, or only a topic's active ones.
     *
     * @param topicOrNull narrow to one topic's active subscriptions, or {@code null} for every subscription
     * @return the subscriptions as a JSON array
     */
    default String listSubscriptionsJson(@Nullable String topicOrNull) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Remove a subscription; false if none matched.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @return whether a subscription was removed
     */
    default boolean unsubscribe(String topic, String subscriptionName) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Pause (false) or resume (true) a subscription; false if none matched.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param active {@code false} to stop deliveries without unsubscribing
     * @return whether a subscription was changed
     */
    default boolean setSubscriptionActive(String topic, String subscriptionName, boolean active) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * A JSON array of per-subscription backlog snapshots, one per registered subscription.
     *
     * @return the snapshots as a JSON array
     */
    default String topicBacklogStatsJson() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Drop ephemeral subscriptions whose owning worker is gone; returns the count removed.
     *
     * @return how many subscriptions were removed
     */
    default long reapEphemeralSubscriptions() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Fan a payload out to every active subscription of {@code topic}. Returns
     * the created jobs as a JSON array — empty when nothing is subscribed.
     *
     * @param topic the topic's name
     * @param payload the encoded message, opaque to the core
     * @param optionsJson one {@code PublishOptions} object as JSON
     * @return the created jobs as a JSON array; empty when nothing is subscribed
     */
    default String publishJson(String topic, byte[] payload, String optionsJson) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Pull up to {@code limit} messages after a log subscription's cursor (oldest
     * first, exclusive), as a JSON array of message views. At-least-once: reading
     * without acking re-delivers.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param limit the most messages to return
     * @return the messages as a JSON array, oldest first
     */
    default String readTopicMessagesJson(String topic, String subscriptionName, long limit) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Advance a log subscription's cursor to {@code cursor} (monotonic); false if nothing moved.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param cursor the id of the last message handled
     * @return whether the cursor moved
     */
    default boolean ackTopicCursor(String topic, String subscriptionName, String cursor) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Lease up to {@code limit} available messages for {@code visibilityMs}, tracking
     * per-message state, as a JSON array of message views. A nack or an expired lease
     * redelivers just that message without blocking its siblings.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param limit the most messages to lease
     * @param visibilityMs how long each lease holds before the message redelivers
     * @return the leased messages as a JSON array
     */
    default String leaseTopicMessagesJson(String topic, String subscriptionName, long limit, long visibilityMs) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Ack one leased message; false if there was no un-acked delivery to ack.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param messageId the leased message's id
     * @return whether a delivery was acked
     */
    default boolean ackMessage(String topic, String subscriptionName, String messageId) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Nack one leased message (redeliver now); false if there was no un-acked delivery to nack.
     *
     * @param topic the topic's name
     * @param subscriptionName the subscription's name within its topic
     * @param messageId the leased message's id
     * @return whether a delivery was nacked
     */
    default boolean nackMessage(String topic, String subscriptionName, String messageId) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * A JSON array of per-log-subscription lag snapshots.
     *
     * @return the snapshots as a JSON array
     */
    default String topicLogStatsJson() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * Declare a log topic (idempotent) so its publishes are retained even with no
     * subscriber. {@code retentionMs} bounds a sub-less backlog; {@code null}
     * keeps messages until consumed.
     *
     * @param name the topic's name
     * @param retentionMs how long a sub-less backlog is kept, or {@code null} to keep until consumed
     */
    default void declareTopic(String name, @Nullable Long retentionMs) {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    /**
     * A JSON array of declared topics.
     *
     * @return the topics as a JSON array
     */
    default String listDeclaredTopicsJson() {
        throw new UnsupportedOperationException(PUBSUB_UNSUPPORTED);
    }

    // ── Workflows ───────────────────────────────────────────────────
    // Optional capability: default to throwing so existing custom backends keep
    // compiling and fail explicitly only when workflows are actually used.
    /** Message every workflow default throws with, so the refusal reads the same everywhere. */
    String WORKFLOWS_UNSUPPORTED = "workflows not supported by this backend";

    /**
     * Record a run and pre-enqueue a job per static step.
     *
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

    /**
     * Record a node's terminal outcome; returns the run's final state, or {@code null}.
     *
     * @param jobId the job's id
     * @param succeeded whether the node's job completed rather than dead-lettered
     * @param error why it failed, or {@code null}
     * @param skipCascade {@code true} to leave dependent nodes alone rather than skipping them
     * @return the run's final state, or {@code null} while it is still live
     */
    default String markWorkflowNodeResult(
            String jobId, boolean succeeded, @Nullable String error, boolean skipCascade) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * A run's state and every node's snapshot.
     *
     * @param runId the workflow run's id
     * @return the status as JSON, or empty when no such run exists
     */
    default Optional<String> getWorkflowStatusJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * A page of workflow runs.
     *
     * @param definitionNameOrNull narrow to one definition, or {@code null} for every one
     * @param stateOrNull narrow to one state, or {@code null} for every state
     * @param limit the page size
     * @param offset how many runs to skip
     * @return the runs as a JSON array
     */
    default String listWorkflowRunsJson(
            @Nullable String definitionNameOrNull, @Nullable String stateOrNull, long limit, long offset) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * A run's summary row, without node detail.
     *
     * @param runId the workflow run's id
     * @return the run as JSON, or empty when no such run exists
     */
    default Optional<String> getWorkflowRunJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * The sub-workflow runs a run spawned.
     *
     * @param runId the workflow run's id
     * @return the children as a JSON array
     */
    default String getWorkflowChildrenJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * A run's graph as the core stored it.
     *
     * @param runId the workflow run's id
     * @return the graph as JSON, or empty when no such run exists
     */
    default Optional<String> getWorkflowDagJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Cancel a run and every node still pending.
     *
     * @param runId the workflow run's id
     */
    default void cancelWorkflowRun(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * The plan a run was submitted with — its steps and their dependencies.
     *
     * @param runId the workflow run's id
     * @return the plan as JSON, or empty when no such run exists
     */
    default Optional<String> getWorkflowPlanJson(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * The workflow node a job belongs to.
     *
     * @param jobId the job's id
     * @return the node as JSON, or empty when the job is not part of a run
     */
    default Optional<String> workflowNodeForJobJson(String jobId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Returns the run's definition name, or empty if the run is absent.
     *
     * @param runId the workflow run's id
     * @return the definition's name, or empty when no such run exists
     */
    default Optional<String> workflowNameForRun(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Create one child job per item a fan-out node produced.
     *
     * @param runId the workflow run's id
     * @param parentNode the fan-out node whose result was split
     * @param childNames the node names to create, one per item
     * @param childPayloads one encoded payload per child, in the same order
     * @param taskName the task's registered name
     * @param queue the queue name
     * @param maxRetries the retry ceiling for each child
     * @param timeoutMs the per-attempt timeout for each child
     * @param priority the dispatch priority for each child
     * @return the created job ids, in child order
     */
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

    /**
     * Whether every child of a fan-out has settled, and with what.
     *
     * @param runId the workflow run's id
     * @param parentNode the fan-out node to check
     * @return the aggregate as JSON, or empty while children are still outstanding
     */
    default Optional<String> checkFanOutCompletionJson(String runId, String parentNode) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Create the job for a node that was deferred at submit.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param payload the encoded payload, resolved now rather than at submit
     * @param taskName the task's registered name
     * @param queue the queue name
     * @param maxRetries the retry ceiling
     * @param timeoutMs the per-attempt timeout
     * @param priority the dispatch priority
     * @return the created job's id
     */
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

    /**
     * Skip every node still pending, after a failure that ends the run.
     *
     * @param runId the workflow run's id
     */
    default void cascadeSkipPending(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Settle the run if every node has, and report the state it settled into.
     *
     * @param runId the workflow run's id
     * @return the final state, or empty while nodes are still outstanding
     */
    default Optional<String> finalizeRunIfTerminal(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Park an approval-gate node until it is resolved.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    default void setWorkflowNodeWaitingApproval(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Settle a parked gate: completed if approved, else failed with {@code error}.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param approved {@code true} to complete the gate, {@code false} to fail it
     * @param error why it was rejected, or {@code null}
     */
    default void resolveWorkflowGate(String runId, String nodeName, boolean approved, @Nullable String error) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Promote a gate / sub-workflow node to running.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    default void setWorkflowNodeRunning(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Mark a node failed.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param error why it failed, or {@code null}
     */
    default void failWorkflowNode(String runId, String nodeName, @Nullable String error) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Mark a node skipped (its condition evaluated false) and cancel any bound job.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    default void skipWorkflowNode(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Mark a node as a cache hit (terminal) without running it.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     */
    default void setWorkflowNodeCacheHit(String runId, String nodeName) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    // ── Saga compensation ─────────────────────────────────────────

    /**
     * Move a failed run into rollback.
     *
     * @param runId the workflow run's id
     */
    default void setWorkflowRunCompensating(String runId) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Record that a run's rollback finished cleanly.
     *
     * @param runId the workflow run's id
     * @param completedAt when it finished, in Unix milliseconds
     */
    default void setWorkflowRunCompensated(String runId, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Record that a run's rollback itself failed.
     *
     * @param runId the workflow run's id
     * @param completedAt when it gave up, in Unix milliseconds
     * @param error why the rollback failed, or {@code null}
     */
    default void setWorkflowRunCompensationFailed(String runId, long completedAt, @Nullable String error) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Settle a run that finished with some nodes failed or skipped.
     *
     * @param runId the workflow run's id
     * @param completedAt when it settled, in Unix milliseconds
     */
    default void setWorkflowRunCompletedWithFailures(String runId, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Bind a node to the job that is rolling it back.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param compensationJobId the rollback job's id
     * @param startedAt when the rollback was dispatched, in Unix milliseconds
     */
    default void setWorkflowNodeCompensationJob(
            String runId, String nodeName, String compensationJobId, long startedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Record that one node was rolled back cleanly.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param completedAt when it finished, in Unix milliseconds
     */
    default void setWorkflowNodeCompensated(String runId, String nodeName, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    /**
     * Record that one node's rollback failed.
     *
     * @param runId the workflow run's id
     * @param nodeName the node's name within its workflow
     * @param error why it failed, or {@code null}
     * @param completedAt when it gave up, in Unix milliseconds
     */
    default void setWorkflowNodeCompensationFailed(
            String runId, String nodeName, @Nullable String error, long completedAt) {
        throw new UnsupportedOperationException(WORKFLOWS_UNSUPPORTED);
    }

    // ── Worker ──────────────────────────────────────────────────────
    /**
     * Start a worker that dispatches jobs to {@code bridge}; returns its control.
     *
     * @param bridge what each claimed job is handed to
     * @param optionsJson the worker options as JSON
     * @return the handle for stopping and inspecting the worker
     */
    WorkerControl startWorker(WorkerBridge bridge, String optionsJson);

    @Override
    void close();
}
