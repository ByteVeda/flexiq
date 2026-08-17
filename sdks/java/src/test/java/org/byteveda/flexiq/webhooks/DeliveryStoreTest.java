package org.byteveda.flexiq.webhooks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** The per-subscription delivery log: newest-first paging, filtering, FIFO cap, and clear. */
@Timeout(30)
class DeliveryStoreTest {

    private static Delivery delivery(String sub, String event, String status) {
        return Delivery.of(sub, new DeliveryContext(event, "task", "job"), status, 1, 200, "ok", 5L, null);
    }

    @Test
    void recordsNewestFirstAndFilters(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open()) {
            DeliveryStore store = new DeliveryStore(queue);
            store.record(delivery("sub", "success", "delivered"));
            store.record(delivery("sub", "retry", "failed"));

            List<Delivery> all = store.listFor("sub", null, null, 50, 0);
            assertEquals(2, all.size());
            assertEquals("retry", all.get(0).event()); // newest first

            assertEquals(1, store.listFor("sub", "failed", null, 50, 0).size());
            assertEquals(1, store.listFor("sub", null, "success", 50, 0).size());
            assertTrue(store.listFor("sub", "dead", null, 50, 0).isEmpty());

            String newestId = all.get(0).id();
            assertTrue(store.get("sub", newestId).isPresent());
            assertTrue(store.get("sub", "missing").isEmpty());
        }
    }

    @Test
    void capsAtTwoHundredDroppingOldest(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open()) {
            DeliveryStore store = new DeliveryStore(queue);
            Delivery oldest = delivery("sub", "success", "delivered");
            store.record(oldest);
            for (int i = 0; i < 205; i++) {
                store.record(delivery("sub", "success", "delivered"));
            }
            assertEquals(200, store.listFor("sub", null, null, 500, 0).size());
            assertTrue(store.get("sub", oldest.id()).isEmpty()); // evicted by the FIFO cap
        }
    }

    @Test
    void deleteForClearsTheLog(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open()) {
            DeliveryStore store = new DeliveryStore(queue);
            store.record(delivery("sub", "success", "delivered"));
            store.deleteFor("sub");
            assertTrue(store.listFor("sub", null, null, 50, 0).isEmpty());
        }
    }
}
