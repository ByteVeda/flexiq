package org.byteveda.taskito.webhooks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.WebhookException;
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
    private static final int MAX_PER_WEBHOOK = 200;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Taskito queue;

    DeliveryStore(Taskito queue) {
        this.queue = queue;
    }

    /** Append {@code delivery} to its subscription's log, trimming to the cap. */
    void record(Delivery delivery) {
        List<Delivery> rows = load(delivery.subscriptionId());
        rows.add(delivery);
        if (rows.size() > MAX_PER_WEBHOOK) {
            rows = new ArrayList<>(rows.subList(rows.size() - MAX_PER_WEBHOOK, rows.size()));
        }
        save(delivery.subscriptionId(), rows);
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
        return queue.getSetting(KEY_PREFIX + subscriptionId)
                .map(DeliveryStore::parse)
                .orElseGet(ArrayList::new);
    }

    private void save(String subscriptionId, List<Delivery> rows) {
        try {
            queue.setSetting(KEY_PREFIX + subscriptionId, JSON.writeValueAsString(rows));
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
