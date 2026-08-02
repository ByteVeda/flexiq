package org.byteveda.taskito.webhooks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.WebhookException;
import org.byteveda.taskito.internal.SettingsDocument;

/** Persists webhooks as a JSON list in the queue's settings key/value store. */
final class WebhookStore {
    private static final String KEY = "taskito.webhooks";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SettingsDocument.Codec<List<Webhook>> CODEC =
            SettingsDocument.codec(WebhookStore::decode, WebhookStore::encode);

    private final Taskito queue;

    WebhookStore(Taskito queue) {
        this.queue = queue;
    }

    List<Webhook> load() {
        return decode(queue.getSetting(KEY));
    }

    /**
     * Apply {@code mutate} to the subscription list without losing a concurrent
     * edit — they all live under one key, so a read-then-write would drop a
     * subscription another dashboard replica had just added.
     */
    <R> R update(SettingsDocument.Mutation<List<Webhook>, R> mutate) {
        return SettingsDocument.update(queue, KEY, CODEC, mutate);
    }

    private static List<Webhook> decode(Optional<String> raw) {
        return raw.map(WebhookStore::parse).orElseGet(ArrayList::new);
    }

    private static String encode(List<Webhook> webhooks) {
        try {
            return JSON.writeValueAsString(webhooks);
        } catch (Exception e) {
            throw new WebhookException("failed to persist webhooks", e);
        }
    }

    private static List<Webhook> parse(String json) {
        try {
            CollectionType type = JSON.getTypeFactory().constructCollectionType(List.class, Webhook.class);
            return JSON.readValue(json, type);
        } catch (Exception e) {
            throw new WebhookException("failed to read webhooks", e);
        }
    }
}
