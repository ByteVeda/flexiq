package org.byteveda.flexiq.dashboard.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.store.OverridesStore.Scope;
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
    void keysMatchTheCrossSdkVectors() {
        assertEquals("overrides:task:send", OverridesStore.key(Scope.TASK, null, "send"));
        assertEquals("overrides:queue:emails", OverridesStore.key(Scope.QUEUE, null, "emails"));
        assertEquals("overrides:ns:7:billing:task:send", OverridesStore.key(Scope.TASK, "billing", "send"));
        assertEquals("overrides:ns:3:a:b:queue:emails", OverridesStore.key(Scope.QUEUE, "a:b", "emails"));
        // The empty namespace is not the default one.
        assertEquals("overrides:ns:0::task:send", OverridesStore.key(Scope.TASK, "", "send"));
        assertEquals(OverridesStore.TASK_PREFIX, OverridesStore.prefix(Scope.TASK, null));
        assertEquals(OverridesStore.QUEUE_PREFIX, OverridesStore.prefix(Scope.QUEUE, null));
    }

    @Test
    void lengthCountsUtf8Bytes() {
        // "é" is one char but two UTF-8 bytes.
        assertEquals("overrides:ns:2:é:task:t", OverridesStore.key(Scope.TASK, "é", "t"));
    }

    @Test
    void namespacedLayoutIsDisjointFromDefault() {
        for (Scope scope : Scope.values()) {
            String defaultPrefix = OverridesStore.prefix(scope, null);
            for (String ns : new String[] {"task", "queue", "", "x:task:"}) {
                assertFalse(OverridesStore.key(scope, ns, "n").startsWith(defaultPrefix));
            }
        }
    }

    @Test
    void namespacesDoNotShareRows() {
        InMemorySettings settings = new InMemorySettings();
        OverridesStore defaults = new OverridesStore(settings);
        OverridesStore billing = new OverridesStore(settings, "billing");

        billing.putTask("send", patch("max_concurrent", 3));
        billing.putQueue("emails", patch("paused", true));

        assertTrue(settings.getSetting("overrides:ns:7:billing:task:send").isPresent());
        assertTrue(settings.getSetting("overrides:ns:7:billing:queue:emails").isPresent());
        assertNull(defaults.getTask("send"));
        assertNull(defaults.getQueue("emails"));
        assertTrue(defaults.taskNames().isEmpty());
        assertTrue(defaults.queueNames().isEmpty());
        assertEquals(Set.of("send"), billing.taskNames());
        assertEquals(Set.of("emails"), billing.queueNames());
        assertTrue(new OverridesStore(settings, "other").taskNames().isEmpty());

        defaults.putTask("send", patch("max_concurrent", 9));
        assertEquals(3, ((Number) billing.getTask("send").get("max_concurrent")).intValue());
        assertFalse(defaults.deleteQueue("emails"));
        assertTrue(billing.deleteQueue("emails"));
        assertTrue(billing.deleteTask("send"));
        assertEquals(9, ((Number) defaults.getTask("send").get("max_concurrent")).intValue());
    }

    @Test
    void deleteAndGet() {
        assertNull(store.getTask("t"));
        store.putTask("t", patch("max_concurrent", 1));
        assertTrue(store.deleteTask("t"));
        assertFalse(store.deleteTask("t"));
    }
}
