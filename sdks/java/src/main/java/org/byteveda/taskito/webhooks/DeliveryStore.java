package org.byteveda.taskito.webhooks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.SettingConflictException;
import org.byteveda.taskito.errors.WebhookException;
import org.byteveda.taskito.internal.SettingsDocument;
import org.byteveda.taskito.logging.TaskitoLogger;
import org.jspecify.annotations.Nullable;

/**
 * Per-subscription webhook delivery log, persisted in the queue's settings KV.
 *
 * <p>Each subscription gets its own JSON list under
 * {@code webhooks:deliveries:<subscriptionId>}, append-only with FIFO eviction
 * once the per-webhook cap is hit — enough to debug recent activity without
 * unbounded growth. Records are stored oldest-first; {@link #listFor} reverses
 * for newest-first paging.
 */
final class DeliveryStore {
    static final String KEY_PREFIX = "webhooks:deliveries:";
    private static final TaskitoLogger LOG = TaskitoLogger.create("webhooks");
    private static final int MAX_PER_WEBHOOK = 200;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SettingsDocument.Codec<List<Delivery>> CODEC =
            SettingsDocument.codec(DeliveryStore::decode, DeliveryStore::encode);

    private final Taskito queue;

    DeliveryStore(Taskito queue) {
        this.queue = queue;
    }

    /**
     * Append {@code delivery} to its subscription's log, trimming to the cap.
     *
     * <p>Written conditionally: two deliveries settling at once would otherwise
     * each append to the list they read and the later write would drop the other.
     *
     * <p>Unlike the admin documents, this key is written once per delivery, so
     * sustained contention is plausible rather than a fault. A log row is
     * diagnostic — losing one must not fail the delivery it describes — so an
     * exhausted retry is logged and dropped rather than propagated.
     */
    void record(Delivery delivery) {
        try {
            SettingsDocument.update(queue, KEY_PREFIX + delivery.subscriptionId(), CODEC, rows -> {
                rows.add(delivery);
                if (rows.size() > MAX_PER_WEBHOOK) {
                    rows.subList(0, rows.size() - MAX_PER_WEBHOOK).clear();
                }
                return rows;
            });
        } catch (SettingConflictException e) {
            LOG.warn("dropped the delivery log row for subscription " + delivery.subscriptionId()
                    + ": the log was rewritten by another writer on every attempt");
        }
    }

    /** Newest-first, optionally filtered by status/event, then paged. */
    List<Delivery> listFor(
            String subscriptionId, @Nullable String statusFilter, @Nullable String eventFilter, int limit, int offset) {
        List<Delivery> rows = load(subscriptionId);
        List<Delivery> out = new ArrayList<>();
        for (int i = rows.size() - 1; i >= 0; i--) {
            Delivery row = rows.get(i);
            if (statusFilter != null && !statusFilter.equals(row.status())) {
                continue;
            }
            if (eventFilter != null && !eventFilter.equals(row.event())) {
                continue;
            }
            out.add(row);
        }
        int from = Math.min(Math.max(offset, 0), out.size());
        int to = limit < 0 ? out.size() : Math.min(from + limit, out.size());
        return new ArrayList<>(out.subList(from, to));
    }

    Optional<Delivery> get(String subscriptionId, String deliveryId) {
        return load(subscriptionId).stream()
                .filter(row -> row.id().equals(deliveryId))
                .findFirst();
    }

    /** Drop the whole log for a subscription (called when the webhook is deleted). */
    void deleteFor(String subscriptionId) {
        queue.deleteSetting(KEY_PREFIX + subscriptionId);
    }

    private List<Delivery> load(String subscriptionId) {
        return decode(queue.getSetting(KEY_PREFIX + subscriptionId));
    }

    private static List<Delivery> decode(Optional<String> raw) {
        return raw.map(DeliveryStore::parse).orElseGet(ArrayList::new);
    }

    private static String encode(List<Delivery> rows) {
        try {
            return JSON.writeValueAsString(rows);
        } catch (Exception e) {
            throw new WebhookException("failed to persist webhook deliveries", e);
        }
    }

    private static List<Delivery> parse(String json) {
        try {
            CollectionType type = JSON.getTypeFactory().constructCollectionType(List.class, Delivery.class);
            return JSON.readValue(json, type);
        } catch (Exception e) {
            // A corrupt log must not wedge the dashboard — start fresh.
            return new ArrayList<>();
        }
    }
}
