package org.byteveda.flexiq.webhooks;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.SerializationException;
import org.byteveda.flexiq.errors.WebhookException;
import org.byteveda.flexiq.events.EnqueuedEvent;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.FlexiQEvent;
import org.byteveda.flexiq.events.GateEvent;
import org.byteveda.flexiq.events.NodeCompensationEvent;
import org.byteveda.flexiq.events.OutcomeEvent;
import org.byteveda.flexiq.events.PredicateEvent;
import org.byteveda.flexiq.events.QueueEvent;
import org.byteveda.flexiq.events.WorkerEvent;
import org.byteveda.flexiq.events.WorkflowEvent;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.middleware.Middleware;
import org.jspecify.annotations.Nullable;

/**
 * Manages webhook subscriptions and dispatches matching job outcomes to them.
 *
 * <p>{@link #attach} registers the manager as queue middleware so outcomes are
 * delivered automatically. {@link #forQueue} builds a standalone manager (no
 * middleware) for CRUD, test, and delivery-history reads — all durable state
 * lives in the shared settings store, so it works without a running worker.
 * Persisted via the queue's settings store.
 */
public final class WebhookManager implements Middleware {
    private static final FlexiQLogger LOG = FlexiQLogger.create("webhooks");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final int SECRET_BYTES = 32;
    // Dispatch runs on the worker's single outcome-drain thread for every job, so
    // it must not re-read + re-parse the settings blob per outcome. Local
    // create/delete invalidate immediately; the TTL bounds staleness from writers
    // in other processes.
    private static final long CACHE_TTL_MS = 30_000;

    private final WebhookStore store;
    private final DeliveryStore deliveryStore;
    private final Deliverer deliverer = new Deliverer();
    private volatile @Nullable CachedHooks cached;

    private WebhookManager(FlexiQ queue) {
        this.store = new WebhookStore(queue);
        this.deliveryStore = new DeliveryStore(queue);
    }

    /**
     * Create a manager and register it on {@code queue} for automatic dispatch.
     *
     * @param queue the queue whose events fire the hooks, and whose settings store
     *     holds them
     * @return the manager, already registered as middleware and event subscriber
     */
    public static WebhookManager attach(FlexiQ queue) {
        WebhookManager manager = new WebhookManager(queue);
        queue.use(manager);
        manager.subscribeQueueEvents(queue);
        return manager;
    }

    /**
     * Subscribe the non-outcome taxonomy (plus per-attempt {@code job.failed})
     * through the queue's event hub. Terminal outcomes keep arriving via the
     * middleware hooks, so they are not double-subscribed. Skipped entirely when
     * the queue has no event hub.
     */
    private void subscribeQueueEvents(FlexiQ queue) {
        try {
            for (EventName name : EventName.values()) {
                if (name.isJobOutcome() && name != EventName.JOB_FAILED) {
                    continue;
                }
                queue.onEvent(name, this::dispatchEvent);
            }
        } catch (UnsupportedOperationException e) {
            // No event hub on this queue implementation; outcome dispatch still works.
        }
    }

    /**
     * Build a manager WITHOUT registering middleware. The dashboard uses this for
     * CRUD, test-ping, replay, and delivery history — none of which need the
     * worker-side dispatch hook, and all of which read/write the shared KV store.
     *
     * @param queue the queue whose settings store holds the hooks
     * @return a manager that reads and writes hooks but dispatches nothing
     */
    public static WebhookManager forQueue(FlexiQ queue) {
        return new WebhookManager(queue);
    }

    /**
     * Mint a fresh URL-safe signing secret (32 random bytes, base64url, no padding).
     *
     * @return the secret, to be shown to the operator once and stored on the hook
     */
    public static String generateSecret() {
        byte[] bytes = new byte[SECRET_BYTES];
        SECURE_RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Persist a draft hook, assigning its id and timestamps.
     *
     * <p>No SSRF check: the programmatic API is trusted developer code and may
     * target an internal service on purpose. Untrusted dashboard input is guarded
     * where it arrives, and again at delivery time.
     *
     * @param spec the draft from {@link Webhook#builder}
     * @return the stored hook, id and timestamps filled in
     */
    public synchronized Webhook create(Webhook.Builder spec) {
        // The programmatic API is trusted developer code (it may target internal
        // services). The SSRF guard runs on untrusted dashboard input (see
        // WebhooksHandlers) and again at delivery time.
        long now = System.currentTimeMillis();
        Webhook hook = new Webhook(
                UUID.randomUUID().toString(),
                spec.url,
                new ArrayList<>(spec.events),
                new ArrayList<>(spec.taskFilters),
                new LinkedHashMap<>(spec.headers),
                spec.secret,
                spec.maxRetries,
                spec.timeoutMs,
                spec.retryBackoff,
                spec.enabled,
                spec.description,
                now,
                now);
        store.update(all -> all.add(hook));
        cached = null;
        return hook;
    }

    /**
     * Every stored hook.
     *
     * @return the hooks, read fresh from the settings store
     */
    public List<Webhook> list() {
        return store.load();
    }

    /**
     * One stored hook.
     *
     * @param id the hook's id
     * @return the hook, or empty if no such hook exists
     */
    public Optional<Webhook> get(String id) {
        return store.load().stream().filter(hook -> hook.id.equals(id)).findFirst();
    }

    /**
     * Apply {@code updates} to the webhook with {@code id} (null fields left
     * unchanged), re-stamp {@code updatedAt}, and persist. Re-validates the
     * (possibly new) URL. Empty if no such webhook exists.
     *
     * @param id the hook's id
     * @param updates the fields to change; the rest are left alone
     * @return the patched hook, or empty if no such hook exists
     */
    public synchronized Optional<Webhook> update(String id, WebhookUpdate updates) {
        Optional<Webhook> merged = store.update(all -> {
            for (int i = 0; i < all.size(); i++) {
                Webhook current = all.get(i);
                if (current.id.equals(id)) {
                    Webhook patched = applyUpdate(current, updates);
                    all.set(i, patched);
                    return Optional.of(patched);
                }
            }
            return Optional.<Webhook>empty();
        });
        if (merged.isPresent()) {
            cached = null;
        }
        return merged;
    }

    /**
     * Replace the webhook's secret with a freshly minted one. Returns the updated
     * hook WITH the new secret so the caller can surface it exactly once.
     *
     * @param id the hook's id
     * @return the patched hook carrying the new secret, or empty if no such hook exists
     */
    public synchronized Optional<Webhook> rotateSecret(String id) {
        return update(id, WebhookUpdate.builder().secret(generateSecret()).build());
    }

    /**
     * Remove a hook and its recorded deliveries.
     *
     * @param id the hook's id
     * @return {@code true} if a hook was removed
     */
    public synchronized boolean delete(String id) {
        boolean removed = store.update(all -> all.removeIf(hook -> hook.id.equals(id)));
        if (removed) {
            deliveryStore.deleteFor(id);
            cached = null;
        }
        return removed;
    }

    /**
     * Synchronously POST a synthetic {@code test} event to the hook and record
     * the delivery. Returns whether the endpoint accepted it (2xx). {@code false}
     * if the webhook is missing or its URL fails the SSRF guard.
     *
     * @param id the hook's id
     * @return whether the endpoint answered 2xx
     */
    public boolean test(String id) {
        Optional<Webhook> hook = get(id);
        if (hook.isEmpty()) {
            return false;
        }
        DeliveryContext ctx = new DeliveryContext("test", null, null);
        return sendSynthetic(hook.get(), ctx, testPayload(id));
    }

    /**
     * Re-send a recorded delivery's payload as a fresh attempt, preserving the
     * original in the log. Returns whether the resend was accepted (2xx).
     * {@code false} if the webhook or delivery is missing, or the URL is unsafe.
     *
     * @param id the hook's id
     * @param deliveryId the recorded delivery to re-send
     * @return whether the endpoint answered 2xx
     */
    public boolean replay(String id, String deliveryId) {
        Optional<Webhook> hook = get(id);
        Optional<Delivery> original = deliveryStore.get(id, deliveryId);
        if (hook.isEmpty() || original.isEmpty()) {
            return false;
        }
        Delivery source = original.get();
        DeliveryContext ctx = new DeliveryContext(source.event(), source.taskName(), source.jobId());
        return sendSynthetic(hook.get(), ctx, replayPayload(source));
    }

    /**
     * A page of one hook's recorded deliveries, newest first.
     *
     * @param id the hook's id
     * @param statusFilter keep only deliveries with this status, or {@code null} for all
     * @param eventFilter keep only deliveries of this event, or {@code null} for all
     * @param limit the page size
     * @param offset how many matching deliveries to skip
     * @return the page
     */
    public List<Delivery> deliveries(
            String id, @Nullable String statusFilter, @Nullable String eventFilter, int limit, int offset) {
        return deliveryStore.listFor(id, statusFilter, eventFilter, limit, offset);
    }

    /**
     * One recorded delivery.
     *
     * @param id the hook's id
     * @param deliveryId the delivery's id
     * @return the delivery, or empty if no such delivery exists
     */
    public Optional<Delivery> delivery(String id, String deliveryId) {
        return deliveryStore.get(id, deliveryId);
    }

    @Override
    public void onCompleted(OutcomeEvent event) {
        dispatch(event);
    }

    @Override
    public void onRetry(OutcomeEvent event) {
        dispatch(event);
    }

    @Override
    public void onDeadLetter(OutcomeEvent event) {
        dispatch(event);
    }

    @Override
    public void onCancel(OutcomeEvent event) {
        dispatch(event);
    }

    private void dispatch(OutcomeEvent event) {
        List<Webhook> hooks = activeHooks();
        if (hooks.isEmpty()) {
            return;
        }
        EventName name = event.name();
        String wire = name.wireName();
        byte[] body = payload(event, wire);
        DeliveryContext ctx = new DeliveryContext(wire, event.taskName, event.jobId);
        for (Webhook hook : hooks) {
            if (hook.enabled && subscribedTo(hook, name) && matches(hook.taskFilters, event.taskName)) {
                deliverOne(hook, body, ctx);
            }
        }
    }

    /**
     * Deliver a non-outcome event (or a per-attempt {@code job.failed}, which
     * carries the full outcome body) to matching hooks. Hooks with a task filter
     * only match task-bearing events.
     */
    void dispatchEvent(FlexiQEvent event) {
        if (event instanceof OutcomeEvent outcome) {
            dispatch(outcome);
            return;
        }
        List<Webhook> hooks = activeHooks();
        if (hooks.isEmpty()) {
            return;
        }
        EventName name = event.name();
        String wire = name.wireName();
        String taskName = taskNameOf(event);
        String jobId = event instanceof EnqueuedEvent enqueued ? enqueued.jobId() : null;
        byte[] body = eventPayload(event, wire);
        DeliveryContext ctx = new DeliveryContext(wire, taskName, jobId);
        for (Webhook hook : hooks) {
            if (hook.enabled && subscribedTo(hook, name) && matches(hook.taskFilters, taskName)) {
                deliverOne(hook, body, ctx);
            }
        }
    }

    /**
     * Whether the hook subscribes to {@code name}. Stored strings are normalized
     * through {@link EventName#fromWire}, so subscriptions persisted before the
     * dotted wire names (e.g. {@code "success"}) still match.
     */
    private static boolean subscribedTo(Webhook hook, EventName name) {
        for (String stored : hook.events) {
            try {
                if (EventName.fromWire(stored) == name) {
                    return true;
                }
            } catch (SerializationException e) {
                // An unknown stored token never matches a live event; skip it.
            }
        }
        return false;
    }

    /** The task an event concerns, or null for events that carry no task. */
    private static @Nullable String taskNameOf(FlexiQEvent event) {
        if (event instanceof EnqueuedEvent enqueued) {
            return enqueued.taskName();
        }
        if (event instanceof PredicateEvent predicate) {
            return predicate.taskName();
        }
        return null;
    }

    private void deliverOne(Webhook hook, byte[] body, DeliveryContext ctx) {
        // Re-validate on every attempt (DNS-rebinding defense): a name that was
        // safe at create time could now resolve to a private address. A failure
        // is recorded and skipped so one bad hook never blocks the rest.
        try {
            WebhookUrlValidator.validate(hook.url);
        } catch (WebhookException e) {
            deliveryStore.record(Delivery.of(hook.id, ctx, Delivery.FAILED, 0, null, null, null, e.getMessage()));
            return;
        }
        try {
            deliverer.deliver(hook, body, ctx, deliveryStore);
        } catch (RuntimeException e) {
            // A bad hook (e.g. malformed URL) must not block the rest. Log the
            // class only — URI parse messages can echo the URL's tokens.
            LOG.warn("webhook " + hook.id + " delivery failed: " + e.getClass().getSimpleName());
        }
    }

    private boolean sendSynthetic(Webhook hook, DeliveryContext ctx, byte[] body) {
        try {
            WebhookUrlValidator.validate(hook.url);
        } catch (WebhookException e) {
            deliveryStore.record(Delivery.of(hook.id, ctx, Delivery.FAILED, 0, null, null, null, e.getMessage()));
            return false;
        }
        int status = deliverer.deliverSync(hook, body, ctx, deliveryStore);
        return status >= 200 && status < 300;
    }

    private static Webhook applyUpdate(Webhook current, WebhookUpdate updates) {
        return new Webhook(
                current.id,
                updates.url() != null ? updates.url() : current.url,
                updates.events() != null ? new ArrayList<>(updates.events()) : current.events,
                updates.taskFilters() != null ? new ArrayList<>(updates.taskFilters()) : current.taskFilters,
                updates.headers() != null ? new LinkedHashMap<>(updates.headers()) : current.headers,
                updates.secret() != null ? updates.secret() : current.secret,
                updates.maxRetries() != null ? updates.maxRetries() : current.maxRetries,
                updates.timeoutMs() != null ? updates.timeoutMs() : current.timeoutMs,
                updates.retryBackoff() != null ? updates.retryBackoff() : current.retryBackoff,
                updates.enabled() != null ? updates.enabled() : current.enabled,
                updates.description() != null ? updates.description() : current.description,
                current.createdAt,
                System.currentTimeMillis());
    }

    private List<Webhook> activeHooks() {
        CachedHooks snapshot = cached;
        long now = System.currentTimeMillis();
        if (snapshot == null || now - snapshot.loadedAt() > CACHE_TTL_MS) {
            snapshot = new CachedHooks(List.copyOf(store.load()), now);
            cached = snapshot;
        }
        return snapshot.hooks();
    }

    /** An immutable webhook list plus when it was read from the store. */
    private record CachedHooks(List<Webhook> hooks, long loadedAt) {}

    /** An empty filter list matches everything; otherwise the task must be listed. */
    private static boolean matches(List<String> filters, @Nullable String taskName) {
        return filters.isEmpty() || filters.contains(taskName);
    }

    static byte[] payload(OutcomeEvent event, String wire) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("event", wire);
        body.put("job_id", event.jobId);
        body.put("task_name", event.taskName);
        body.put("error", event.error);
        body.put("retry_count", event.retryCount);
        body.put("timed_out", event.timedOut);
        body.put("duration_ms", event.durationMs());
        return encode(body);
    }

    /** The per-type snake_case body of a non-outcome event's delivery. */
    static byte[] eventPayload(FlexiQEvent event, String wire) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("event", wire);
        if (event instanceof EnqueuedEvent enqueued) {
            body.put("job_id", enqueued.jobId());
            body.put("task_name", enqueued.taskName());
            body.put("queue", enqueued.queue());
        } else if (event instanceof QueueEvent queueEvent) {
            body.put("queue", queueEvent.queue());
        } else if (event instanceof WorkerEvent workerEvent) {
            body.put("queues", workerEvent.queues());
        } else if (event instanceof WorkflowEvent workflowEvent) {
            body.put("run_id", workflowEvent.runId());
            body.put("workflow", workflowEvent.workflowName());
            body.put("error", workflowEvent.error());
        } else if (event instanceof GateEvent gate) {
            body.put("run_id", gate.runId());
            body.put("node", gate.nodeName());
        } else if (event instanceof NodeCompensationEvent compensation) {
            body.put("run_id", compensation.runId());
            body.put("node", compensation.nodeName());
            body.put("error", compensation.error());
        } else if (event instanceof PredicateEvent predicate) {
            body.put("task_name", predicate.taskName());
            body.put("reason", predicate.reason());
        }
        return encode(body);
    }

    private static byte[] testPayload(String webhookId) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("event", "test");
        body.put("webhook_id", webhookId);
        return encode(body);
    }

    private static byte[] replayPayload(Delivery source) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("event", source.event());
        body.put("task_name", source.taskName());
        body.put("job_id", source.jobId());
        body.put("replay_of", source.id());
        return encode(body);
    }

    private static byte[] encode(Map<String, Object> body) {
        try {
            return JSON.writeValueAsBytes(body);
        } catch (Exception e) {
            throw new WebhookException("webhook payload encoding failed", e);
        }
    }
}
