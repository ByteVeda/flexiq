package org.byteveda.taskito.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.byteveda.taskito.TaskitoException;
import org.byteveda.taskito.spi.QueueBackend;
import org.byteveda.taskito.spi.WorkerBridge;
import org.byteveda.taskito.spi.WorkerControl;

/**
 * A pure-Java, in-memory {@link QueueBackend} for fast unit tests — no JNI, no
 * disk. It runs jobs on a small polling worker (dispatch → complete/fail → retry
 * or dead-letter), and supports inspection, admin, settings, locks, and topic
 * pub/sub (fan-out, per-subscriber dedup, ephemeral reaping).
 *
 * <p>Not supported: workflows (use a native backend). Periodic registration is
 * recorded but never fires. Behaviour approximates the core for testing handlers,
 * retries, and dead-lettering — it is not the production scheduler.
 */
public final class InMemoryQueueBackend implements QueueBackend {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DEFAULT_QUEUE = "default";

    /**
     * Reap grace for freshly registered ephemeral rows, mirroring the core's
     * {@code EPHEMERAL_SUBSCRIPTION_GRACE_MS}: a starting worker writes its
     * subscriptions before its liveness is visible everywhere, and a concurrent
     * reap must not race that gap.
     */
    private static final long EPHEMERAL_SUBSCRIPTION_GRACE_MS = 60_000;

    private final Map<String, JobRec> jobs = new ConcurrentHashMap<>();
    private final Map<String, String> settings = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Object>> periodics = new ConcurrentHashMap<>();
    private final Map<String, LockRec> locks = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> dead = new CopyOnWriteArrayList<>();
    private final Map<String, List<Map<String, Object>>> logs = new ConcurrentHashMap<>();
    private final java.util.Set<String> paused = ConcurrentHashMap.newKeySet();
    private final List<InMemoryWorker> workers = new CopyOnWriteArrayList<>();
    private final Map<String, SubscriptionRec> subscriptions = new ConcurrentHashMap<>();
    private final java.util.Set<String> liveWorkers = ConcurrentHashMap.newKeySet();
    private final AtomicLong seq = new AtomicLong();

    private static long now() {
        return System.currentTimeMillis();
    }

    // ── Producer ────────────────────────────────────────────────────

    @Override
    public String enqueue(String taskName, byte[] payload, String optionsJson) {
        return enqueueOne(taskName, payload, readNode(optionsJson));
    }

    @Override
    public String[] enqueueMany(String taskName, byte[][] payloads, String optionsJson) {
        JsonNode array = readNode(optionsJson);
        String[] ids = new String[payloads.length];
        for (int i = 0; i < payloads.length; i++) {
            JsonNode opts = array.isArray() && i < array.size() ? array.get(i) : null;
            ids[i] = enqueueOne(taskName, payloads[i], opts);
        }
        return ids;
    }

    // synchronized so the unique-key scan + insert is atomic against claimNext
    // and concurrent enqueues.
    private synchronized String enqueueOne(String taskName, byte[] payload, JsonNode opts) {
        String uniqueKey = text(opts, "uniqueKey");
        if (uniqueKey != null) {
            for (JobRec existing : jobs.values()) {
                if (uniqueKey.equals(existing.uniqueKey)
                        && ("pending".equals(existing.status) || "running".equals(existing.status))) {
                    return existing.id;
                }
            }
        }
        JobRec job = new JobRec();
        job.enqueueSeq = seq.incrementAndGet();
        job.id = "im-" + job.enqueueSeq;
        job.taskName = taskName;
        job.payload = payload;
        job.queue = optText(opts, "queue", DEFAULT_QUEUE);
        job.priority = optInt(opts, "priority", 0);
        job.maxRetries = optInt(opts, "maxRetries", 0);
        job.timeoutMs = optLong(opts, "timeoutMs", 0);
        job.uniqueKey = uniqueKey;
        job.metadata = text(opts, "metadata");
        job.namespace = text(opts, "namespace");
        job.notes = text(opts, "notes");
        long createdAt = now();
        job.createdAt = createdAt;
        job.scheduledAt = createdAt + optLong(opts, "delayMs", 0);
        JsonNode expiresMs = opts == null ? null : opts.get("expiresMs");
        if (expiresMs != null && !expiresMs.isNull()) {
            job.expiresAt = createdAt + Math.max(0, expiresMs.asLong());
        }
        JsonNode resultTtlMs = opts == null ? null : opts.get("resultTtlMs");
        if (resultTtlMs != null && !resultTtlMs.isNull()) {
            job.resultTtlMs = resultTtlMs.asLong();
        }
        jobs.put(job.id, job);
        return job.id;
    }

    // This monitor guards a JobRec's fields — the map is concurrent, its records
    // are not. Every method that mutates one, or snapshots more than one, takes it.
    // Without it a poller sees claimNext's half-applied expiry: already cancelled,
    // error not yet assigned.
    @Override
    public synchronized Optional<String> getJobJson(String jobId) {
        JobRec job = jobs.get(jobId);
        return job == null ? Optional.empty() : Optional.of(toJson(jobView(job)));
    }

    @Override
    public synchronized Optional<byte[]> getResult(String jobId) {
        JobRec job = jobs.get(jobId);
        if (job == null || resultExpired(job)) {
            return Optional.empty();
        }
        return Optional.ofNullable(job.result);
    }

    /** Whether the job's result outlived its retention TTL (no TTL = kept). */
    private static boolean resultExpired(JobRec job) {
        return job.resultTtlMs != null && job.completedAt != null && job.completedAt + job.resultTtlMs < now();
    }

    // synchronized so a pending→cancelled transition can't race claimNext's
    // pending→running (which is also on the backend monitor).
    @Override
    public synchronized boolean cancel(String jobId) {
        JobRec job = jobs.get(jobId);
        if (job != null && "pending".equals(job.status)) {
            job.status = "cancelled";
            job.completedAt = now();
            return true;
        }
        return false;
    }

    // synchronized like cancel(): the running→pending transition must not race
    // claimNext's pending→running.
    @Override
    public synchronized boolean requeueJob(String jobId) {
        JobRec job = jobs.get(jobId);
        if (job == null || !"running".equals(job.status)) {
            return false;
        }
        job.status = "pending";
        job.scheduledAt = now();
        job.startedAt = null;
        job.completedAt = null;
        job.error = null;
        // A stale cancel request must not kill the fresh attempt.
        job.cancelRequested = false;
        return true;
    }

    @Override
    public synchronized boolean requestCancel(String jobId) {
        JobRec job = jobs.get(jobId);
        if (job != null && "running".equals(job.status)) {
            job.cancelRequested = true;
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean isCancelRequested(String jobId) {
        JobRec job = jobs.get(jobId);
        return job != null && job.cancelRequested;
    }

    @Override
    public synchronized void setProgress(String jobId, int progress) {
        JobRec job = jobs.get(jobId);
        if (job != null) {
            job.progress = Math.max(0, Math.min(100, progress));
        }
    }

    // ── Inspection ──────────────────────────────────────────────────

    @Override
    public String statsJson() {
        return toJson(statsFor(null));
    }

    @Override
    public String statsByQueueJson(String queue) {
        return toJson(statsFor(queue));
    }

    @Override
    public long countPendingByQueue(String queue) {
        return count("pending", queue);
    }

    @Override
    public String statsAllQueuesJson() {
        Map<String, Object> out = new LinkedHashMap<>();
        jobs.values().stream().map(j -> j.queue).distinct().forEach(q -> out.put(q, statsFor(q)));
        return toJson(out);
    }

    @Override
    public synchronized String listJobsJson(String filterJson) {
        JsonNode filter = readNode(filterJson);
        String status = text(filter, "status");
        String queue = text(filter, "queue");
        String task = text(filter, "task");
        int limit = optInt(filter, "limit", Integer.MAX_VALUE);
        int offset = optInt(filter, "offset", 0);
        List<Map<String, Object>> views = new ArrayList<>();
        for (JobRec job : jobs.values()) {
            if (status != null && !status.equals(job.status)) {
                continue;
            }
            if (queue != null && !queue.equals(job.queue)) {
                continue;
            }
            if (task != null && !task.equals(job.taskName)) {
                continue;
            }
            views.add(jobView(job));
        }
        int from = Math.min(offset, views.size());
        int to = limit == Integer.MAX_VALUE ? views.size() : Math.min(from + limit, views.size());
        return toJson(views.subList(from, to));
    }

    @Override
    public synchronized String listJobsAfterJson(String filterJson, String afterOrNull) {
        JsonNode filter = readNode(filterJson);
        String status = text(filter, "status");
        String queue = text(filter, "queue");
        String task = text(filter, "task");
        int limit = Math.max(0, optInt(filter, "limit", Integer.MAX_VALUE));

        List<JobRec> matching = new ArrayList<>();
        for (JobRec job : jobs.values()) {
            if (status != null && !status.equals(job.status)) {
                continue;
            }
            if (queue != null && !queue.equals(job.queue)) {
                continue;
            }
            if (task != null && !task.equals(job.taskName)) {
                continue;
            }
            matching.add(job);
        }
        return keysetPage(matching, limit, afterOrNull, job -> job.createdAt);
    }

    @Override
    public synchronized String listArchivedAfterJson(long limit, String afterOrNull) {
        List<JobRec> archived = new ArrayList<>();
        for (JobRec job : jobs.values()) {
            if (job.completedAt != null) {
                archived.add(job);
            }
        }
        // Clamp before narrowing: the native handlers reject a negative limit, and
        // subList would throw on one rather than return an empty page.
        long clamped = Math.max(0, limit);
        return keysetPage(
                archived,
                (int) Math.min(clamped, Integer.MAX_VALUE),
                afterOrNull,
                job -> job.completedAt == null ? job.createdAt : job.completedAt);
    }

    /**
     * Order by {@code (sortKey, id)} descending, seek past the cursor, take a
     * page — the same contract the native backends implement, so a test against
     * this backend exercises the real pagination semantics rather than a stub.
     */
    private static String keysetPage(
            List<JobRec> rows, int limit, String afterOrNull, java.util.function.ToLongFunction<JobRec> sortKey) {
        rows.sort((a, b) -> {
            int byKey = Long.compare(sortKey.applyAsLong(b), sortKey.applyAsLong(a));
            return byKey != 0 ? byKey : b.id.compareTo(a.id);
        });
        if (afterOrNull != null) {
            int split = afterOrNull.indexOf(':');
            if (split < 0) {
                throw new IllegalArgumentException("invalid cursor: " + afterOrNull);
            }
            long cursorKey = Long.parseLong(afterOrNull.substring(0, split));
            String cursorId = afterOrNull.substring(split + 1);
            rows.removeIf(job -> {
                long key = sortKey.applyAsLong(job);
                return key > cursorKey || (key == cursorKey && job.id.compareTo(cursorId) >= 0);
            });
        }
        List<JobRec> page = rows.subList(0, Math.min(limit, rows.size()));
        List<Map<String, Object>> views = new ArrayList<>();
        for (JobRec job : page) {
            views.add(jobView(job));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("items", views);
        // A short page is the last one.
        result.put(
                "nextCursor",
                page.size() < limit || page.isEmpty()
                        ? null
                        : sortKey.applyAsLong(page.get(page.size() - 1)) + ":" + page.get(page.size() - 1).id);
        return toJson(result);
    }

    @Override
    public String jobErrorsJson(String jobId) {
        return "[]";
    }

    @Override
    public String metricsJson(String taskNameOrNull, long sinceMs) {
        return "[]";
    }

    @Override
    public String listWorkersJson() {
        return "[]";
    }

    // ── Admin ───────────────────────────────────────────────────────

    @Override
    public String listDeadJson(long limit, long offset) {
        int from = (int) Math.min(offset, dead.size());
        int to = (int) Math.min(from + limit, dead.size());
        return toJson(new ArrayList<>(dead.subList(from, to)));
    }

    @Override
    public synchronized String retryDead(String deadId) {
        for (Map<String, Object> entry : dead) {
            if (deadId.equals(entry.get("id"))) {
                JobRec source = jobs.get((String) entry.get("originalJobId"));
                dead.remove(entry);
                JobRec job = new JobRec();
                job.enqueueSeq = seq.incrementAndGet();
                job.id = "im-" + job.enqueueSeq;
                job.taskName = (String) entry.get("taskName");
                job.queue = (String) entry.get("queue");
                job.payload = source == null ? new byte[0] : source.payload;
                job.maxRetries = source == null ? 0 : source.maxRetries;
                // Preserve the rest of the original job's options so a retry
                // behaves like the original, not a stripped-down job.
                if (source != null) {
                    job.priority = source.priority;
                    job.timeoutMs = source.timeoutMs;
                    job.uniqueKey = source.uniqueKey;
                    job.metadata = source.metadata;
                    job.namespace = source.namespace;
                }
                job.createdAt = now();
                job.scheduledAt = job.createdAt;
                jobs.put(job.id, job);
                return job.id;
            }
        }
        throw new TaskitoException("no dead-letter entry: " + deadId);
    }

    @Override
    public boolean deleteDead(String deadId) {
        return dead.removeIf(entry -> deadId.equals(entry.get("id")));
    }

    @Override
    public long purgeDead(long olderThanMs) {
        int before = dead.size();
        dead.removeIf(entry -> ((Number) entry.get("failedAt")).longValue() < olderThanMs);
        return before - dead.size();
    }

    @Override
    public String listDeadByTaskJson(String taskName, long limit, long offset) {
        List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> entry : dead) {
            if (taskName.equals(entry.get("taskName"))) {
                matches.add(entry);
            }
        }
        int from = (int) Math.min(offset, matches.size());
        int to = (int) Math.min(from + limit, matches.size());
        return toJson(new ArrayList<>(matches.subList(from, to)));
    }

    @Override
    public long purgeDeadByTask(String taskName) {
        // Count the entries actually removed (not a before/after size diff, which
        // a concurrent onFail append could skew).
        List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> entry : dead) {
            if (taskName.equals(entry.get("taskName"))) {
                matches.add(entry);
            }
        }
        dead.removeAll(matches);
        return matches.size();
    }

    @Override
    public synchronized long purgeCompleted(long olderThanMs) {
        List<String> remove = new ArrayList<>();
        for (JobRec job : jobs.values()) {
            if ("complete".equals(job.status) && job.completedAt != null && job.completedAt < olderThanMs) {
                remove.add(job.id);
            }
        }
        remove.forEach(jobs::remove);
        return remove.size();
    }

    @Override
    public void pauseQueue(String queue) {
        paused.add(queue);
    }

    @Override
    public void resumeQueue(String queue) {
        paused.remove(queue);
    }

    @Override
    public String listPausedQueuesJson() {
        return toJson(new ArrayList<>(paused));
    }

    @Override
    public Optional<String> getSetting(String key) {
        return Optional.ofNullable(settings.get(key));
    }

    @Override
    public void setSetting(String key, String value) {
        settings.put(key, value);
    }

    @Override
    public boolean deleteSetting(String key) {
        return settings.remove(key) != null;
    }

    @Override
    public String listSettingsJson() {
        return toJson(new LinkedHashMap<>(settings));
    }

    // ── Logs ────────────────────────────────────────────────────────

    @Override
    public void writeTaskLog(String jobId, String taskName, String level, String message, String extraOrNull) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("id", "log-" + seq.incrementAndGet());
        entry.put("jobId", jobId);
        entry.put("taskName", taskName);
        entry.put("level", level);
        entry.put("message", message);
        entry.put("extra", extraOrNull);
        entry.put("loggedAt", now());
        logs.computeIfAbsent(jobId, k -> new CopyOnWriteArrayList<>()).add(entry);
    }

    @Override
    public String getTaskLogsJson(String jobId) {
        return toJson(new ArrayList<>(logs.getOrDefault(jobId, new ArrayList<>())));
    }

    // ── Locks ───────────────────────────────────────────────────────

    @Override
    public synchronized boolean acquireLock(String name, String ownerId, long ttlMs) {
        LockRec held = locks.get(name);
        if (held != null && held.expiresAt > now() && !held.ownerId.equals(ownerId)) {
            return false;
        }
        locks.put(name, new LockRec(name, ownerId, now(), now() + ttlMs));
        return true;
    }

    @Override
    public synchronized boolean releaseLock(String name, String ownerId) {
        LockRec held = locks.get(name);
        if (held != null && held.ownerId.equals(ownerId)) {
            locks.remove(name);
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean extendLock(String name, String ownerId, long ttlMs) {
        LockRec held = locks.get(name);
        if (held != null && held.ownerId.equals(ownerId)) {
            held.expiresAt = now() + ttlMs;
            return true;
        }
        return false;
    }

    @Override
    public Optional<String> lockInfoJson(String name) {
        LockRec held = locks.get(name);
        if (held == null || held.expiresAt <= now()) {
            return Optional.empty();
        }
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("lockName", held.name);
        view.put("ownerId", held.ownerId);
        view.put("acquiredAt", held.acquiredAt);
        view.put("expiresAt", held.expiresAt);
        return Optional.of(toJson(view));
    }

    // ── Periodic ────────────────────────────────────────────────────
    // Catalog only — the in-memory backend records periodic tasks but never
    // fires them. list/delete/pause exercise the same management surface as JNI.

    @Override
    public long registerPeriodic(
            String name, String taskName, String cron, byte[] args, String queue, String timezone, boolean enabled) {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("name", name);
        rec.put("taskName", taskName);
        rec.put("cronExpr", cron);
        rec.put("queue", queue);
        rec.put("enabled", enabled);
        rec.put("lastRun", null);
        rec.put("nextRun", now());
        rec.put("timezone", timezone);
        periodics.put(name, rec);
        return now();
    }

    @Override
    public String listPeriodicJson() {
        return toJson(new ArrayList<>(periodics.values()));
    }

    @Override
    public boolean deletePeriodic(String name) {
        return periodics.remove(name) != null;
    }

    @Override
    public boolean setPeriodicEnabled(String name, boolean enabled) {
        Map<String, Object> rec = periodics.get(name);
        if (rec == null) {
            return false;
        }
        rec.put("enabled", enabled);
        return true;
    }

    // ── Pub/Sub ─────────────────────────────────────────────────────

    @Override
    public synchronized void registerSubscription(
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            String ownerWorkerIdOrNull) {
        // Reachable via either overload; nulls take the queue defaults.
        registerSubscription(topic, subscriptionName, taskName, queue, durable, ownerWorkerIdOrNull, null, null, null);
    }

    @Override
    public synchronized void registerSubscription(
            String topic,
            String subscriptionName,
            String taskName,
            String queue,
            boolean durable,
            String ownerWorkerIdOrNull,
            Integer priority,
            Integer maxRetries,
            Long timeoutMs) {
        // Mirrors the native guard: an ownerless ephemeral row would never be
        // reaped yet keep receiving deliveries.
        if (!durable && ownerWorkerIdOrNull == null) {
            throw new TaskitoException("an ephemeral subscription (durable=false) requires ownerWorkerId");
        }
        // Upsert mirrors the core: routing (task, queue, durable, owner) and the
        // persisted delivery settings are replaced while active and creation time
        // survive — re-declaring never resumes a pause or refreshes the reap grace.
        subscriptions.compute(subscriptionKey(topic, subscriptionName), (key, existing) -> {
            SubscriptionRec rec = new SubscriptionRec(
                    topic,
                    subscriptionName,
                    taskName,
                    queue,
                    durable,
                    ownerWorkerIdOrNull,
                    priority,
                    maxRetries,
                    timeoutMs,
                    existing == null ? seq.incrementAndGet() : existing.createdSeq,
                    existing == null ? now() : existing.createdAt);
            if (existing != null) {
                rec.active = existing.active;
            }
            return rec;
        });
    }

    @Override
    public String listSubscriptionsJson(String topicOrNull) {
        List<Map<String, Object>> views = new ArrayList<>();
        for (SubscriptionRec sub : subscriptionsInOrder()) {
            if (topicOrNull != null && !(topicOrNull.equals(sub.topic) && sub.active)) {
                continue;
            }
            views.add(subscriptionView(sub));
        }
        return toJson(views);
    }

    @Override
    public boolean unsubscribe(String topic, String subscriptionName) {
        return subscriptions.remove(subscriptionKey(topic, subscriptionName)) != null;
    }

    @Override
    public boolean setSubscriptionActive(String topic, String subscriptionName, boolean active) {
        SubscriptionRec sub = subscriptions.get(subscriptionKey(topic, subscriptionName));
        if (sub == null) {
            return false;
        }
        sub.active = active;
        return true;
    }

    @Override
    public long reapEphemeralSubscriptions() {
        long cutoff = now() - EPHEMERAL_SUBSCRIPTION_GRACE_MS;
        long removed = 0;
        for (Map.Entry<String, SubscriptionRec> entry : subscriptions.entrySet()) {
            SubscriptionRec sub = entry.getValue();
            if (sub.ownerWorkerId == null || liveWorkers.contains(sub.ownerWorkerId) || sub.createdAt >= cutoff) {
                continue;
            }
            // Conditional removal: a replacement registered under the same key
            // after this scan must survive the reap.
            if (subscriptions.remove(entry.getKey(), sub)) {
                removed++;
            }
        }
        return removed;
    }

    /** Test seam: age a subscription past the reap grace window. */
    void backdateSubscription(String topic, String subscriptionName, long ageMs) {
        SubscriptionRec sub = subscriptions.get(subscriptionKey(topic, subscriptionName));
        if (sub != null) {
            sub.createdAt -= ageMs;
        }
    }

    @Override
    public synchronized String publishJson(String topic, byte[] payload, String optionsJson) {
        JsonNode opts = readNode(optionsJson);
        List<Map<String, Object>> views = new ArrayList<>();
        for (SubscriptionRec sub : subscriptionsInOrder()) {
            if (!topic.equals(sub.topic) || !sub.active) {
                continue;
            }
            // enqueueOne's unique-key scan gives keyed publishes per-subscriber
            // dedup: a republished key resolves to the existing delivery.
            String id = enqueueOne(sub.taskName, payload, deliveryOptions(opts, sub));
            views.add(jobView(jobs.get(id)));
        }
        return toJson(views);
    }

    private static String subscriptionKey(String topic, String name) {
        return topic + "\0" + name;
    }

    /** Registration order (creation seq), matching the core's stable listing order. */
    private List<SubscriptionRec> subscriptionsInOrder() {
        List<SubscriptionRec> ordered = new ArrayList<>(subscriptions.values());
        ordered.sort(Comparator.comparingLong(sub -> sub.createdSeq));
        return ordered;
    }

    private static Map<String, Object> subscriptionView(SubscriptionRec sub) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("topic", sub.topic);
        view.put("subscriptionName", sub.name);
        view.put("taskName", sub.taskName);
        view.put("queue", sub.queue);
        view.put("active", sub.active);
        view.put("durable", sub.durable);
        view.put("ownerWorkerId", sub.ownerWorkerId);
        return view;
    }

    /**
     * One delivery's enqueue options: routing from the subscription, delivery
     * settings resolving publish override → the subscription row's persisted
     * setting → zero (so a producer-only publish applies the subscriber's own
     * settings without knowing the task), the idempotency key salted per
     * subscriber (the unique-key scan is global, so a raw key would dedup away
     * all but one delivery), and the caller's notes stamped with
     * {@code topic}/{@code subscription}.
     */
    private static ObjectNode deliveryOptions(JsonNode opts, SubscriptionRec sub) {
        ObjectNode delivery = JSON.createObjectNode();
        delivery.put("queue", sub.queue);
        delivery.put("priority", resolveDeliveryInt(opts, "priority", sub.priority));
        delivery.put("maxRetries", resolveDeliveryInt(opts, "maxRetries", sub.maxRetries));
        delivery.put("timeoutMs", resolveDeliveryLong(opts, "timeoutMs", sub.timeoutMs));
        delivery.put("delayMs", optLong(opts, "delayMs", 0));
        // Publish-level expiry and result retention pass straight through; the
        // enqueue path resolves them (expiresMs → an absolute expiry).
        copyIfSet(opts, delivery, "expiresMs");
        copyIfSet(opts, delivery, "resultTtlMs");
        String idempotencyKey = text(opts, "idempotencyKey");
        if (idempotencyKey != null) {
            delivery.put("uniqueKey", idempotencyKey + "::" + sub.name);
        }
        String metadata = text(opts, "metadata");
        if (metadata != null) {
            delivery.put("metadata", metadata);
        }
        delivery.put("notes", deliveryNotes(text(opts, "notes"), sub));
        return delivery;
    }

    /** Copy a field from the publish options into the delivery, when present. */
    private static void copyIfSet(JsonNode opts, ObjectNode delivery, String field) {
        JsonNode value = opts == null ? null : opts.get(field);
        if (value != null && !value.isNull()) {
            delivery.put(field, value.asLong());
        }
    }

    private static int resolveDeliveryInt(JsonNode opts, String field, Integer rowSetting) {
        JsonNode override = opts == null ? null : opts.get(field);
        if (override != null && !override.isNull()) {
            return override.asInt();
        }
        return rowSetting == null ? 0 : rowSetting;
    }

    private static long resolveDeliveryLong(JsonNode opts, String field, Long rowSetting) {
        JsonNode override = opts == null ? null : opts.get(field);
        if (override != null && !override.isNull()) {
            return override.asLong();
        }
        return rowSetting == null ? 0L : rowSetting;
    }

    /** The caller's notes (if any) with {@code topic} and {@code subscription} stamped in. */
    private static String deliveryNotes(String callerNotes, SubscriptionRec sub) {
        ObjectNode notes = JSON.createObjectNode();
        if (callerNotes != null) {
            JsonNode parsed = readNode(callerNotes);
            if (parsed.isObject()) {
                notes.setAll((ObjectNode) parsed);
            }
        }
        notes.put("topic", sub.topic);
        notes.put("subscription", sub.name);
        return notes.toString();
    }

    // ── Worker ──────────────────────────────────────────────────────

    @Override
    public WorkerControl startWorker(WorkerBridge bridge, String optionsJson) {
        JsonNode options = readNode(optionsJson);
        String workerId = "im-worker-" + seq.incrementAndGet();
        // Mark the worker live before its ephemeral subscriptions exist (like
        // the native path), so a concurrent reap never sees an owned row
        // without a live owner. Roll back liveness if registration fails.
        liveWorkers.add(workerId);
        try {
            registerWorkerSubscriptions(workerId, options);
        } catch (RuntimeException e) {
            liveWorkers.remove(workerId);
            throw e;
        }
        InMemoryWorker worker = new InMemoryWorker(bridge, parseQueues(optionsJson), workerId);
        workers.add(worker);
        worker.start();
        return worker;
    }

    /** Register the worker's declared subscriptions; ephemeral ones bind to its id. */
    private void registerWorkerSubscriptions(String workerId, JsonNode options) {
        JsonNode specs = options == null ? null : options.get("subscriptions");
        if (specs == null || !specs.isArray()) {
            return;
        }
        for (JsonNode spec : specs) {
            boolean durable = spec.path("durable").asBoolean(true);
            registerSubscription(
                    text(spec, "topic"),
                    text(spec, "subscriptionName"),
                    text(spec, "taskName"),
                    optText(spec, "queue", DEFAULT_QUEUE),
                    durable,
                    durable ? null : workerId,
                    boxedInt(spec, "priority"),
                    boxedInt(spec, "maxRetries"),
                    boxedLong(spec, "timeoutMs"));
        }
    }

    /** The worker's queue filter from its options JSON; empty = consume every queue. */
    private java.util.Set<String> parseQueues(String optionsJson) {
        java.util.Set<String> queues = new java.util.HashSet<>();
        try {
            JsonNode node = JSON.readTree(optionsJson).get("queues");
            if (node != null && node.isArray()) {
                node.forEach(q -> queues.add(q.asText()));
            }
        } catch (Exception ignored) {
            // Missing or malformed options — fall back to consuming every queue.
        }
        return queues;
    }

    @Override
    public void close() {
        workers.forEach(InMemoryWorker::stop);
        workers.clear();
    }

    // ── Workflows (unsupported in-memory) ───────────────────────────

    private static <T> T unsupported() {
        throw new UnsupportedOperationException(
                "the in-memory backend does not support workflows; use a native backend");
    }

    @Override
    public String submitWorkflow(
            String name,
            int version,
            String stepsJson,
            String[] payloadNames,
            byte[][] payloads,
            String queueDefault,
            String paramsJson,
            String[] deferredNames,
            String parentRunId,
            String parentNodeName) {
        return unsupported();
    }

    @Override
    public String markWorkflowNodeResult(String jobId, boolean succeeded, String error, boolean skipCascade) {
        return unsupported();
    }

    @Override
    public Optional<String> getWorkflowStatusJson(String runId) {
        return unsupported();
    }

    @Override
    public void cancelWorkflowRun(String runId) {
        unsupported();
    }

    @Override
    public Optional<String> getWorkflowPlanJson(String runId) {
        return unsupported();
    }

    @Override
    public Optional<String> workflowNodeForJobJson(String jobId) {
        return unsupported();
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
        return unsupported();
    }

    @Override
    public Optional<String> checkFanOutCompletionJson(String runId, String parentNode) {
        return unsupported();
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
        return unsupported();
    }

    @Override
    public void cascadeSkipPending(String runId) {
        unsupported();
    }

    @Override
    public Optional<String> finalizeRunIfTerminal(String runId) {
        return unsupported();
    }

    // ── Internals ───────────────────────────────────────────────────

    /** Claim a dispatchable pending job (status → running); null if none right now. */
    private synchronized JobRec claimNext(java.util.Set<String> queues) {
        long now = now();
        JobRec best = null;
        for (JobRec job : jobs.values()) {
            if (!"pending".equals(job.status) || job.scheduledAt > now || paused.contains(job.queue)) {
                continue;
            }
            // Skip expired jobs, cancelling them — mirrors the core's dequeue,
            // which archives an expired candidate as cancelled.
            if (job.expiresAt != null && now > job.expiresAt) {
                job.status = "cancelled";
                job.completedAt = now;
                job.error = "expired before execution";
                continue;
            }
            // Honor the worker's queue filter; empty filter = every queue.
            if (!queues.isEmpty() && !queues.contains(job.queue)) {
                continue;
            }
            if (best == null || claimsBefore(job, best)) {
                best = job;
            }
        }
        if (best != null) {
            best.status = "running";
            best.startedAt = now;
        }
        return best;
    }

    /** Production dequeue order: priority DESC, then FIFO (scheduled_at ASC, insertion order on ties). */
    private static boolean claimsBefore(JobRec candidate, JobRec current) {
        if (candidate.priority != current.priority) {
            return candidate.priority > current.priority;
        }
        if (candidate.scheduledAt != current.scheduledAt) {
            return candidate.scheduledAt < current.scheduledAt;
        }
        return candidate.enqueueSeq < current.enqueueSeq;
    }

    private synchronized void onComplete(JobRec job, byte[] result) {
        job.result = result;
        job.status = "complete";
        job.completedAt = now();
    }

    /** Mirrors the core: a non-retryable failure skips the budget and dead-letters. */
    private synchronized void onFail(JobRec job, String error, boolean retryable) {
        if (retryable && job.retryCount < job.maxRetries) {
            job.retryCount++;
            job.status = "pending";
            job.startedAt = null;
            job.scheduledAt = now();
        } else {
            job.status = "dead";
            job.error = error;
            job.completedAt = now();
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", "dead-" + seq.incrementAndGet());
            entry.put("originalJobId", job.id);
            entry.put("queue", job.queue);
            entry.put("taskName", job.taskName);
            entry.put("error", error);
            entry.put("retryCount", job.retryCount);
            entry.put("failedAt", now());
            entry.put("metadata", job.metadata);
            entry.put("dlqRetryCount", 0);
            dead.add(entry);
        }
    }

    private synchronized void onCancel(JobRec job) {
        job.status = "cancelled";
        job.completedAt = now();
    }

    private Map<String, Object> statsFor(String queue) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("pending", count("pending", queue));
        out.put("running", count("running", queue));
        out.put("completed", count("complete", queue));
        out.put("failed", count("failed", queue));
        out.put("dead", count("dead", queue));
        out.put("cancelled", count("cancelled", queue));
        return out;
    }

    private long count(String status, String queue) {
        return jobs.values().stream()
                .filter(j -> status.equals(j.status) && (queue == null || queue.equals(j.queue)))
                .count();
    }

    private static Map<String, Object> jobView(JobRec job) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("id", job.id);
        view.put("queue", job.queue);
        view.put("taskName", job.taskName);
        view.put("status", job.status);
        view.put("priority", job.priority);
        view.put("createdAt", job.createdAt);
        view.put("scheduledAt", job.scheduledAt);
        view.put("startedAt", job.startedAt);
        view.put("completedAt", job.completedAt);
        view.put("retryCount", job.retryCount);
        view.put("maxRetries", job.maxRetries);
        view.put("timeoutMs", job.timeoutMs);
        view.put("progress", job.progress);
        view.put("error", job.error);
        view.put("uniqueKey", job.uniqueKey);
        view.put("namespace", job.namespace);
        view.put("metadata", job.metadata);
        view.put("notes", job.notes);
        return view;
    }

    private static String toJson(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (Exception e) {
            throw new TaskitoException("failed to encode in-memory view", e);
        }
    }

    private static JsonNode readNode(String json) {
        if (json == null || json.isEmpty()) {
            return JSON.nullNode();
        }
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            throw new TaskitoException("failed to read in-memory options", e);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static String optText(JsonNode node, String field, String fallback) {
        String value = text(node, field);
        return value == null ? fallback : value;
    }

    private static int optInt(JsonNode node, String field, int fallback) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? fallback : value.asInt();
    }

    private static long optLong(JsonNode node, String field, long fallback) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? fallback : value.asLong();
    }

    /** A nullable int field — absent/null stays {@code null} so it reads as "queue default". */
    private static Integer boxedInt(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? null : value.asInt();
    }

    /** A nullable long field — absent/null stays {@code null} so it reads as "queue default". */
    private static Long boxedLong(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? null : value.asLong();
    }

    /** A stored job. */
    private static final class JobRec {
        String id;
        long enqueueSeq;
        String queue = DEFAULT_QUEUE;
        String taskName;
        byte[] payload;
        byte[] result;
        volatile String status = "pending";
        int priority;
        long createdAt;
        long scheduledAt;
        Long startedAt;
        Long completedAt;
        int retryCount;
        int maxRetries;
        long timeoutMs;
        Integer progress;
        String error;
        Long expiresAt;
        Long resultTtlMs;
        String uniqueKey;
        String namespace;
        String metadata;
        String notes;
        volatile boolean cancelRequested;
    }

    /** A topic subscription row. Keyed by {@link #subscriptionKey}. */
    private static final class SubscriptionRec {
        final String topic;
        final String name;
        final String taskName;
        final String queue;
        final boolean durable;
        final String ownerWorkerId;
        // The subscriber task's persisted delivery settings; null = queue default.
        final Integer priority;
        final Integer maxRetries;
        final Long timeoutMs;
        final long createdSeq;
        volatile long createdAt;
        volatile boolean active = true;

        SubscriptionRec(
                String topic,
                String name,
                String taskName,
                String queue,
                boolean durable,
                String ownerWorkerId,
                Integer priority,
                Integer maxRetries,
                Long timeoutMs,
                long createdSeq,
                long createdAt) {
            this.topic = topic;
            this.name = name;
            this.taskName = taskName;
            this.queue = queue;
            this.durable = durable;
            this.ownerWorkerId = ownerWorkerId;
            this.priority = priority;
            this.maxRetries = maxRetries;
            this.timeoutMs = timeoutMs;
            this.createdSeq = createdSeq;
            this.createdAt = createdAt;
        }
    }

    private static final class LockRec {
        final String name;
        final String ownerId;
        final long acquiredAt;
        long expiresAt;

        LockRec(String name, String ownerId, long acquiredAt, long expiresAt) {
            this.name = name;
            this.ownerId = ownerId;
            this.acquiredAt = acquiredAt;
            this.expiresAt = expiresAt;
        }
    }

    /** Polls for dispatchable jobs and drives outcomes back through the bridge. */
    private final class InMemoryWorker implements WorkerControl {
        private final WorkerBridge bridge;
        private final java.util.Set<String> queues;
        private final String workerId;
        private final Map<Long, String> inFlight = new ConcurrentHashMap<>();
        // Dispatch time per in-flight token, so outcomes report a duration like
        // the native runtime does.
        private final Map<Long, Long> dispatchedAt = new ConcurrentHashMap<>();
        private volatile boolean running = true;
        private Thread loop;

        InMemoryWorker(WorkerBridge bridge, java.util.Set<String> queues, String workerId) {
            this.bridge = bridge;
            this.queues = queues;
            this.workerId = workerId;
        }

        void start() {
            loop = new Thread(this::run, "taskito-inmemory-worker");
            loop.setDaemon(true);
            loop.start();
        }

        private void run() {
            while (running) {
                JobRec job;
                while (running && (job = claimNext(queues)) != null) {
                    long token = seq.incrementAndGet();
                    inFlight.put(token, job.id);
                    dispatchedAt.put(token, System.nanoTime());
                    bridge.onJob(token, job.id, job.taskName, job.payload);
                }
                sleep();
            }
        }

        private void sleep() {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }

        /**
         * The job this token may still settle, or null when the outcome is stale.
         * The core fences on status — its {@code complete} filters {@code Running}
         * and reports the job as missing otherwise — so an attempt whose claim was
         * released (by {@code requeueJob}, a reaper, or a cancel) must not settle the
         * job it no longer owns.
         *
         * <p>Callers must hold the backend monitor across both this check and the
         * state transition that follows: {@code requeueJob} and {@code claimNext} take
         * the same monitor, so checking outside it would let a concurrent requeue turn
         * the job pending between the check and the settlement.
         */
        private JobRec settleable(long token) {
            String jobId = inFlight.remove(token);
            JobRec job = jobId == null ? null : jobs.get(jobId);
            if (job != null && "running".equals(job.status)) {
                return job;
            }
            dispatchedAt.remove(token); // no outcome will report this token's duration
            return null;
        }

        @Override
        public void completeJob(long token, byte[] result) {
            JobRec job;
            long wallTimeNs;
            synchronized (InMemoryQueueBackend.this) {
                job = settleable(token);
                if (job == null) {
                    return;
                }
                wallTimeNs = wallTimeOf(token);
                onComplete(job, result);
            }
            // Outside the monitor: onOutcome runs user listeners, which may enqueue,
            // block, or call back into the backend.
            bridge.onOutcome("success", job.id, job.taskName, null, job.retryCount, false, wallTimeNs);
        }

        @Override
        public void failJob(long token, String error, boolean retryable) {
            JobRec job;
            long wallTimeNs;
            boolean willRetry;
            synchronized (InMemoryQueueBackend.this) {
                job = settleable(token);
                if (job == null) {
                    return;
                }
                willRetry = retryable && job.retryCount < job.maxRetries;
                wallTimeNs = wallTimeOf(token);
                onFail(job, error, retryable);
            }
            bridge.onOutcome(
                    willRetry ? "retry" : "dead", job.id, job.taskName, error, job.retryCount, false, wallTimeNs);
        }

        @Override
        public void cancelJob(long token) {
            JobRec job;
            long wallTimeNs;
            synchronized (InMemoryQueueBackend.this) {
                job = settleable(token);
                if (job == null) {
                    return;
                }
                wallTimeNs = wallTimeOf(token);
                onCancel(job);
            }
            bridge.onOutcome("cancelled", job.id, job.taskName, null, job.retryCount, false, wallTimeNs);
        }

        /** Elapsed nanos since this token was dispatched; 0 when it wasn't (unmeasured). */
        private long wallTimeOf(long token) {
            Long started = dispatchedAt.remove(token);
            return started == null ? 0L : System.nanoTime() - started;
        }

        @Override
        public void stop() {
            running = false;
            if (loop != null) {
                loop.interrupt();
            }
            liveWorkers.remove(workerId);
            // A graceful stop removes its own ephemeral subscriptions directly:
            // unlike the generic reap, the owner is known-dead here, so the
            // fresh-row registration grace does not apply. Conditional removal
            // spares a row re-registered under a new owner meanwhile.
            for (Map.Entry<String, SubscriptionRec> entry : subscriptions.entrySet()) {
                SubscriptionRec sub = entry.getValue();
                if (workerId.equals(sub.ownerWorkerId)) {
                    subscriptions.remove(entry.getKey(), sub);
                }
            }
        }

        @Override
        public void close() {
            stop();
        }
    }
}
