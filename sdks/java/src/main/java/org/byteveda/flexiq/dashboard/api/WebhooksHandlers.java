package org.byteveda.flexiq.dashboard.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.support.DashboardError;
import org.byteveda.flexiq.errors.SerializationException;
import org.byteveda.flexiq.errors.WebhookException;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.webhooks.Webhook;
import org.byteveda.flexiq.webhooks.WebhookManager;
import org.byteveda.flexiq.webhooks.WebhookUpdate;
import org.byteveda.flexiq.webhooks.WebhookUrlValidator;
import org.jspecify.annotations.Nullable;

/**
 * Webhook subscription CRUD + test/replay + delivery history for the dashboard.
 *
 * <p>Uses a middleware-free {@link WebhookManager} — all state lives in the
 * shared settings KV, so this works without a running worker. Every response is
 * secret-safe: {@link Contract#webhook} masks header values and never emits the
 * raw secret; only {@link #create} and {@link #rotateSecret} surface it, once.
 */
public final class WebhooksHandlers {
    private static final int DEFAULT_DELIVERY_LIMIT = 50;
    private static final int MAX_DELIVERY_LIMIT = 200;

    private final WebhookManager manager;

    /**
     * Handlers over one queue's webhooks.
     *
     * @param queue whose settings store holds the hooks; no middleware is registered,
     *     so these routes work with no worker running
     */
    public WebhooksHandlers(FlexiQ queue) {
        this.manager = WebhookManager.forQueue(queue);
    }

    /**
     * Every stored hook.
     *
     * @return the hooks, header values masked and secrets withheld
     */
    public Object list() {
        return manager.list().stream().map(Contract::webhook).collect(Collectors.toList());
    }

    /**
     * One stored hook.
     *
     * @param id the hook's id
     * @return the hook, secret withheld, or {@code null} for a 404
     */
    public @Nullable Object get(String id) {
        return manager.get(id).map(Contract::webhook).orElse(null);
    }

    /**
     * Create a hook from untrusted dashboard input, SSRF-guarding its URL.
     *
     * @param body the request body: {@code url} required, plus {@code events},
     *     {@code headers}, {@code task_filter}, {@code max_retries},
     *     {@code timeout_seconds}, {@code retry_backoff}, {@code enabled},
     *     {@code description} and {@code secret}
     * @return the created hook, the one response that carries its secret
     */
    public Object create(Map<String, Object> body) {
        String url = requireString(body, "url");
        validateUrl(url);
        Webhook.Builder spec = Webhook.builder(url);
        List<EventName> events = parseEvents(body.get("events"));
        spec.on(events.toArray(new EventName[0]));
        headers(body.get("headers")).forEach(spec::header);
        List<String> taskFilters = taskFilters(body.get("task_filter"));
        if (taskFilters != null) {
            spec.taskFilters(taskFilters.toArray(new String[0]));
        }
        spec.maxRetries(intOr(body.get("max_retries"), "max_retries", 3));
        spec.timeoutMs(timeoutMs(body.get("timeout_seconds"), 10_000));
        spec.retryBackoff(retryBackoff(body.get("retry_backoff"), Webhook.DEFAULT_RETRY_BACKOFF));
        if (body.containsKey("enabled")) {
            spec.enabled(bool(body.get("enabled"), "enabled"));
        }
        String description = optionalString(body, "description");
        if (description != null) {
            spec.description(description);
        }
        spec.secret(resolveSecret(body));
        return Contract.webhookWithSecret(manager.create(spec));
    }

    /**
     * Patch a hook; absent keys are left alone, and a new URL is SSRF-guarded.
     *
     * @param id the hook's id
     * @param body the fields to change, named as in {@link #create}
     * @return the patched hook, secret withheld, or {@code null} for a 404
     */
    public @Nullable Object update(String id, Map<String, Object> body) {
        WebhookUpdate.Builder update = WebhookUpdate.builder();
        if (body.containsKey("url")) {
            String url = requireString(body, "url");
            validateUrl(url);
            update.url(url);
        }
        if (body.containsKey("events")) {
            update.events(parseEvents(body.get("events")).stream()
                    .map(EventName::wireName)
                    .collect(Collectors.toList()));
        }
        if (body.containsKey("task_filter")) {
            update.taskFilters(taskFilters(body.get("task_filter")));
        }
        if (body.containsKey("headers")) {
            update.headers(headers(body.get("headers")));
        }
        if (body.containsKey("max_retries")) {
            update.maxRetries(intOr(body.get("max_retries"), "max_retries", 3));
        }
        if (body.containsKey("timeout_seconds")) {
            update.timeoutMs(timeoutMs(body.get("timeout_seconds"), 10_000));
        }
        if (body.containsKey("retry_backoff")) {
            update.retryBackoff(retryBackoff(body.get("retry_backoff"), Webhook.DEFAULT_RETRY_BACKOFF));
        }
        if (body.containsKey("enabled")) {
            update.enabled(bool(body.get("enabled"), "enabled"));
        }
        if (body.containsKey("description")) {
            update.description(optionalString(body, "description"));
        }
        if (body.containsKey("secret")) {
            update.secret(optionalString(body, "secret"));
        }
        return manager.update(id, update.build()).map(Contract::webhook).orElse(null);
    }

    /**
     * Remove a hook and its recorded deliveries.
     *
     * @param id the hook's id
     * @return {@code {"deleted": true}}, or {@code null} for a 404
     */
    public @Nullable Object delete(String id) {
        return manager.delete(id) ? Map.of("deleted", true) : null;
    }

    /**
     * Mint a new signing secret for a hook.
     *
     * @param id the hook's id
     * @return the hook carrying its new secret — surfaced once, so the operator must
     *     copy it now — or {@code null} for a 404
     */
    public @Nullable Object rotateSecret(String id) {
        return manager.rotateSecret(id).map(Contract::webhookWithSecret).orElse(null);
    }

    /**
     * Post a synthetic {@code test} event to the hook, synchronously.
     *
     * @param id the hook's id
     * @return whether the endpoint answered 2xx, under {@code delivered}, or
     *     {@code null} for a 404
     */
    public @Nullable Object test(String id) {
        if (manager.get(id).isEmpty()) {
            return null;
        }
        return Map.of("delivered", manager.test(id));
    }

    /**
     * A page of one hook's recorded deliveries.
     *
     * @param id the hook's id
     * @param query {@code status}, {@code event}, {@code limit} (capped at 200) and
     *     {@code offset}, each optional
     * @return the page
     */
    public Object deliveries(String id, Map<String, String> query) {
        int limit = Math.min(intQuery(query, "limit", DEFAULT_DELIVERY_LIMIT), MAX_DELIVERY_LIMIT);
        int offset = intQuery(query, "offset", 0);
        return manager.deliveries(id, query.get("status"), query.get("event"), limit, offset).stream()
                .map(Contract::delivery)
                .collect(Collectors.toList());
    }

    /**
     * One recorded delivery.
     *
     * @param id the hook's id
     * @param deliveryId the delivery's id
     * @return the delivery, or {@code null} for a 404
     */
    public @Nullable Object delivery(String id, String deliveryId) {
        return manager.delivery(id, deliveryId).map(Contract::delivery).orElse(null);
    }

    /**
     * Re-send a recorded delivery, keeping the original in the log.
     *
     * @param id the hook's id
     * @param deliveryId the delivery to re-send
     * @return whether the endpoint answered 2xx, under {@code replayed}, or
     *     {@code null} for a 404
     */
    public @Nullable Object replayDelivery(String id, String deliveryId) {
        if (manager.get(id).isEmpty() || manager.delivery(id, deliveryId).isEmpty()) {
            return null;
        }
        return Map.of("replayed", manager.replay(id, deliveryId));
    }

    // ---- body parsing ------------------------------------------------------

    private static void validateUrl(String url) {
        try {
            WebhookUrlValidator.validate(url);
        } catch (WebhookException e) {
            throw DashboardError.badRequest("unsafe_webhook_url");
        }
    }

    private static @Nullable String resolveSecret(Map<String, Object> body) {
        if (isTruthy(body.get("generate_secret"))) {
            return WebhookManager.generateSecret();
        }
        return optionalString(body, "secret");
    }

    private static List<EventName> parseEvents(@Nullable Object raw) {
        if (raw == null) {
            return List.of();
        }
        if (!(raw instanceof List<?> list)) {
            throw DashboardError.badRequest("events_must_be_list");
        }
        List<EventName> events = new ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof String name)) {
                throw DashboardError.badRequest("events_must_be_strings");
            }
            events.add(parseEvent(name));
        }
        return events;
    }

    /** Accept dotted wire names, the legacy outcome aliases, and the old UPPER enum names. */
    private static EventName parseEvent(String name) {
        try {
            return EventName.fromWire(name);
        } catch (SerializationException e) {
            // Fall through to the pre-taxonomy UPPER enum spelling.
        }
        try {
            return EventName.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw DashboardError.badRequest("unknown_event");
        }
    }

    private static Map<String, String> headers(@Nullable Object raw) {
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Map<?, ?> map)) {
            throw DashboardError.badRequest("headers_must_be_object");
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key) || !(entry.getValue() instanceof String value)) {
                throw DashboardError.badRequest("headers_must_be_strings");
            }
            out.put(key, value);
        }
        return out;
    }

    private static String requireString(Map<String, Object> body, String key) {
        Object value = body.get(key);
        if (!(value instanceof String string) || string.isEmpty()) {
            throw DashboardError.badRequest("missing_" + key);
        }
        return string;
    }

    private static @Nullable String optionalString(Map<String, Object> body, String key) {
        Object value = body.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String string)) {
            throw DashboardError.badRequest("invalid_" + key);
        }
        return string;
    }

    private static int intOr(@Nullable Object value, String name, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean || !(value instanceof Number number) || number.intValue() < 0) {
            throw DashboardError.badRequest("invalid_" + name);
        }
        return number.intValue();
    }

    private static long timeoutMs(@Nullable Object seconds, long fallbackMs) {
        if (seconds == null) {
            return fallbackMs;
        }
        if (seconds instanceof Boolean || !(seconds instanceof Number number) || number.doubleValue() <= 0) {
            throw DashboardError.badRequest("invalid_timeout_seconds");
        }
        return Math.round(number.doubleValue() * 1000);
    }

    /**
     * The contract carries {@code task_filter} as a list, but a scalar is still
     * accepted so a client written against the older shape keeps working.
     * {@code null} clears the restriction.
     */
    private static @Nullable List<String> taskFilters(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String single) {
            return List.of(single);
        }
        if (!(value instanceof List<?> raw)) {
            throw DashboardError.badRequest("invalid_task_filter");
        }
        List<String> names = new ArrayList<>(raw.size());
        for (Object entry : raw) {
            if (!(entry instanceof String name)) {
                throw DashboardError.badRequest("invalid_task_filter");
            }
            names.add(name);
        }
        return names;
    }

    private static double retryBackoff(@Nullable Object value, double fallback) {
        if (value == null) {
            return fallback;
        }
        // A base of zero or less would collapse the curve to no wait at all.
        if (value instanceof Boolean || !(value instanceof Number number) || number.doubleValue() <= 0) {
            throw DashboardError.badRequest("invalid_retry_backoff");
        }
        return number.doubleValue();
    }

    private static boolean bool(Object value, String name) {
        if (!(value instanceof Boolean flag)) {
            throw DashboardError.badRequest("invalid_" + name);
        }
        return flag;
    }

    private static boolean isTruthy(@Nullable Object value) {
        return value instanceof Boolean flag && flag;
    }

    private static int intQuery(Map<String, String> query, String key, int fallback) {
        String value = query.get(key);
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Math.max(0, Integer.parseInt(value));
        } catch (NumberFormatException e) {
            throw DashboardError.badRequest("invalid_" + key);
        }
    }
}
