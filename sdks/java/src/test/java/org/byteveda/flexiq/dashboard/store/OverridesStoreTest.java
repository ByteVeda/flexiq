package org.byteveda.flexiq.dashboard.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.support.DashboardError;
import org.junit.jupiter.api.Test;

class OverridesStoreTest {

    private final OverridesStore store = new OverridesStore(new InMemorySettings());

    private static Map<String, Object> patch(String key, Object value) {
        Map<String, Object> m = new HashMap<>();
        m.put(key, value);
        return m;
    }

    @Test
    void normalisesTaskRow() {
        Map<String, Object> row = store.putTask("email.send", patch("max_concurrent", 5));
        assertEquals("email.send", row.get("task_name"));
        assertEquals(5, ((Number) row.get("max_concurrent")).intValue());
        assertEquals(false, row.get("paused"));
        assertNull(row.get("timeout"));
        assertTrue(row.containsKey("updated_at"));
    }

    @Test
    void mergesAndClearsFields() {
        store.putTask("t", patch("max_concurrent", 5));
        Map<String, Object> merged = store.putTask("t", patch("paused", true));
        assertEquals(5, ((Number) merged.get("max_concurrent")).intValue());
        assertEquals(true, merged.get("paused"));

        Map<String, Object> cleared = store.putTask("t", patch("max_concurrent", null));
        assertNull(cleared.get("max_concurrent"));
        assertEquals(true, cleared.get("paused"));
    }

    @Test
    void validatesTaskFields() {
        assertThrows(DashboardError.class, () -> store.putTask("t", patch("rate_limit", "100")));
        assertThrows(DashboardError.class, () -> store.putTask("t", patch("max_concurrent", -1)));
        assertThrows(DashboardError.class, () -> store.putTask("t", patch("timeout", 0)));
        assertThrows(DashboardError.class, () -> store.putTask("t", patch("retry_backoff", -1)));
        assertThrows(DashboardError.class, () -> store.putTask("t", patch("priority", 1.5)));
        assertThrows(DashboardError.class, () -> store.putTask("t", patch("paused", "yes")));
        assertThrows(DashboardError.class, () -> store.putTask("t", patch("bogus", 1)));
    }

    @Test
    void acceptsValidRateLimit() {
        Map<String, Object> row = store.putTask("t", patch("rate_limit", "100/minute"));
        assertEquals("100/minute", row.get("rate_limit"));
    }

    @Test
    void queueRejectsTaskOnlyFields() {
        assertThrows(DashboardError.class, () -> store.putQueue("q", patch("max_retries", 3)));
        Map<String, Object> row = store.putQueue("q", patch("max_concurrent", 2));
        assertEquals("q", row.get("queue_name"));
        assertEquals(2, ((Number) row.get("max_concurrent")).intValue());
    }

    @Test
    void deleteAndGet() {
        assertNull(store.getTask("t"));
        store.putTask("t", patch("max_concurrent", 1));
        assertTrue(store.deleteTask("t"));
        assertFalse(store.deleteTask("t"));
    }
}
