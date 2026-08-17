package org.byteveda.flexiq.webhooks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.WebhookException;
import org.byteveda.flexiq.internal.SettingsDocument;
import org.jspecify.annotations.Nullable;

/**
 * Persists webhook subscriptions in the cross-SDK layout: one JSON array under
 * {@code webhooks:subscriptions}, snake_case fields, timestamps in Unix ms and
 * the timeout in seconds.
 *
 * <p>Any shell driving the same queue reads and writes that one document, so a
 * row may carry fields this one does not model. Every mutation rewrites the
 * whole array, so those fields are captured on read and merged back on write —
 * dropping them would silently destroy another shell's configuration.
 */
final class WebhookStore {
    private static final String KEY = "webhooks:subscriptions";

    /** Where this shell kept its hooks before the layout was shared. */
    private static final String LEGACY_KEY = "flexiq.webhooks";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final double DEFAULT_TIMEOUT_SECONDS = 10.0;

    /** Fields this shell owns; everything else in a row is carried through untouched. */
    private static final Set<String> MODELLED = Set.of(
            "id",
            "url",
            "events",
            "task_filter",
            "headers",
            "secret",
            "max_retries",
            "timeout_seconds",
            "retry_backoff",
            "enabled",
            "description",
            "created_at",
            "updated_at");

    private final FlexiQ queue;
    private final SettingsDocument.Codec<List<Webhook>> codec = SettingsDocument.codec(this::decode, this::encode);

    /** Unmodelled fields per subscription id, refreshed on every decode. */
    private volatile Map<String, Map<String, Object>> unmodelled = Map.of();

    private volatile boolean legacyMerged;

    WebhookStore(FlexiQ queue) {
        this.queue = queue;
    }

    List<Webhook> load() {
        mergeLegacy();
        return decode(queue.getSetting(KEY));
    }

    /**
     * Apply {@code mutate} to the subscription list without losing a concurrent
     * edit — they all live under one key, so a read-then-write would drop a
     * subscription another dashboard replica had just added.
     */
    <R> R update(SettingsDocument.Mutation<List<Webhook>, R> mutate) {
        mergeLegacy();
        return SettingsDocument.update(queue, KEY, codec, mutate);
    }

    /**
     * Fold hooks written under the pre-contract key into the shared document,
     * once per store. A canonical row wins on an id collision — it is the one
     * every other shell can already see.
     */
    private void mergeLegacy() {
        if (legacyMerged) {
            return;
        }
        Optional<String> stored = queue.getSetting(LEGACY_KEY);
        if (stored.isPresent()) {
            List<Webhook> legacy = parseLegacy(stored.get());
            SettingsDocument.update(queue, KEY, codec, all -> {
                Set<String> canonical = new HashSet<>();
                for (Webhook hook : all) {
                    canonical.add(hook.id);
                }
                boolean added = false;
                for (Webhook hook : legacy) {
                    if (!canonical.contains(hook.id)) {
                        all.add(hook);
                        added = true;
                    }
                }
                return added;
            });
            queue.deleteSetting(LEGACY_KEY);
        }
        legacyMerged = true;
    }

    private List<Webhook> decode(Optional<String> raw) {
        if (raw.isEmpty()) {
            unmodelled = Map.of();
            return new ArrayList<>();
        }
        List<Map<String, Object>> rows = parseRows(raw.get());
        Map<String, Map<String, Object>> extras = new LinkedHashMap<>();
        List<Webhook> webhooks = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Webhook hook = decodeRow(row);
            webhooks.add(hook);
            Map<String, Object> extra = unmodelledFields(row);
            if (!extra.isEmpty()) {
                extras.put(hook.id, extra);
            }
        }
        unmodelled = extras;
        return webhooks;
    }

    private String encode(List<Webhook> webhooks) {
        Map<String, Map<String, Object>> extras = unmodelled;
        List<Map<String, Object>> rows = new ArrayList<>(webhooks.size());
        for (Webhook hook : webhooks) {
            Map<String, Object> row = new LinkedHashMap<>(extras.getOrDefault(hook.id, Map.of()));
            row.putAll(encodeRow(hook));
            rows.add(row);
        }
        try {
            return JSON.writeValueAsString(rows);
        } catch (Exception e) {
            throw new WebhookException("failed to persist webhooks", e);
        }
    }

    private static Map<String, Object> encodeRow(Webhook hook) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", hook.id);
        row.put("url", hook.url);
        row.put("events", hook.events);
        row.put("task_filter", hook.taskFilters.isEmpty() ? null : hook.taskFilters);
        row.put("headers", hook.headers);
        row.put("secret", hook.secret);
        row.put("max_retries", hook.maxRetries);
        row.put("timeout_seconds", hook.timeoutMs / 1000.0);
        row.put("retry_backoff", hook.retryBackoff);
        row.put("enabled", hook.enabled);
        row.put("description", hook.description);
        row.put("created_at", hook.createdAt);
        row.put("updated_at", hook.updatedAt);
        return row;
    }

    private static Webhook decodeRow(Map<String, Object> row) {
        double timeoutSeconds = number(row.get("timeout_seconds"), DEFAULT_TIMEOUT_SECONDS);
        return new Webhook(
                string(row.get("id")),
                string(row.get("url")),
                strings(row.get("events")),
                strings(row.get("task_filter")),
                headers(row.get("headers")),
                nullableString(row.get("secret")),
                (int) number(row.get("max_retries"), 3),
                Math.round(timeoutSeconds * 1000),
                number(row.get("retry_backoff"), Webhook.DEFAULT_RETRY_BACKOFF),
                !Boolean.FALSE.equals(row.get("enabled")),
                nullableString(row.get("description")),
                (long) number(row.get("created_at"), 0),
                (long) number(row.get("updated_at"), 0));
    }

    /** Whatever another shell wrote into this row that this one does not model. */
    private static Map<String, Object> unmodelledFields(Map<String, Object> row) {
        Map<String, Object> extra = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (!MODELLED.contains(entry.getKey())) {
                extra.put(entry.getKey(), entry.getValue());
            }
        }
        return extra;
    }

    private static String string(@Nullable Object value) {
        if (!(value instanceof String text)) {
            throw new WebhookException("webhook subscription is missing a string field");
        }
        return text;
    }

    private static @Nullable String nullableString(@Nullable Object value) {
        return value instanceof String text ? text : null;
    }

    /** Tolerates the scalar a pre-contract row may carry where a list belongs. */
    private static List<String> strings(@Nullable Object value) {
        if (value instanceof String single) {
            return List.of(single);
        }
        if (!(value instanceof List<?> raw)) {
            return List.of();
        }
        List<String> out = new ArrayList<>(raw.size());
        for (Object entry : raw) {
            if (entry instanceof String text) {
                out.add(text);
            }
        }
        return out;
    }

    private static Map<String, String> headers(@Nullable Object value) {
        if (!(value instanceof Map<?, ?> raw)) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            if (entry.getKey() instanceof String name && entry.getValue() instanceof String header) {
                out.put(name, header);
            }
        }
        return out;
    }

    private static double number(@Nullable Object value, double fallback) {
        return value instanceof Number number ? number.doubleValue() : fallback;
    }

    private static List<Map<String, Object>> parseRows(String json) {
        try {
            return JSON.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
        } catch (Exception e) {
            throw new WebhookException("failed to read webhooks", e);
        }
    }

    private static List<Webhook> parseLegacy(String json) {
        try {
            CollectionType type = JSON.getTypeFactory().constructCollectionType(List.class, Webhook.class);
            return JSON.readValue(json, type);
        } catch (Exception e) {
            throw new WebhookException("failed to read webhooks", e);
        }
    }
}
