package org.byteveda.taskito.contrib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.webhooks.Delivery;
import org.byteveda.taskito.webhooks.Webhook;
import org.byteveda.taskito.webhooks.WebhookManager;
import org.byteveda.taskito.webhooks.WebhookUpdate;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WebhookTest {

    @Test
    void crudRoundTrip(@TempDir Path dir) {
        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            WebhookManager webhooks = WebhookManager.attach(queue);

            Webhook created = webhooks.create(Webhook.builder("http://example/hook")
                    .on(EventName.SUCCESS, EventName.DEAD)
                    .secret("s"));
            assertNotNull(created.id);
            assertEquals(2, created.events.size());

            assertEquals(1, webhooks.list().size());
            assertTrue(webhooks.get(created.id).isPresent());
            // The builder stores dotted wire names, not the legacy aliases.
            assertTrue(webhooks.get(created.id).get().events.contains("job.completed"));
            assertTrue(webhooks.get(created.id).get().events.contains("job.dead"));

            assertTrue(webhooks.delete(created.id));
            assertTrue(webhooks.list().isEmpty());
            assertFalse(webhooks.delete(created.id));
        }
    }

    @Test
    @Timeout(30)
    void legacyStoredSubscriptionStillMatchesOutcomes(@TempDir Path dir) throws Exception {
        Task<Integer> task = Task.of("wh.ok", Integer.class);
        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            WebhookManager webhooks = WebhookManager.attach(queue);
            Webhook created =
                    webhooks.create(Webhook.builder("http://127.0.0.1:9/hook").on(EventName.SUCCESS));
            // Rewrite the stored subscription to the legacy alias, as persisted
            // by builds that predate the dotted wire names.
            webhooks.update(
                    created.id,
                    WebhookUpdate.builder().events(List.of("success")).build());

            queue.enqueue(task, 1);
            Worker worker = queue.worker().handle(task, p -> p).start();
            try (worker) {
                // The loopback URL fails the SSRF guard, but only a MATCHED hook
                // records a delivery — which is exactly what we assert on.
                List<Delivery> deliveries = List.of();
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (deliveries.isEmpty() && System.nanoTime() < deadline) {
                    deliveries = webhooks.deliveries(created.id, null, null, 50, 0);
                    Thread.sleep(50);
                }
                if (deliveries.isEmpty()) {
                    fail("legacy 'success' subscription never matched the job.completed outcome");
                }
                assertEquals("job.completed", deliveries.get(0).event());
                assertEquals("wh.ok", deliveries.get(0).taskName());
            }
        }
    }
}
