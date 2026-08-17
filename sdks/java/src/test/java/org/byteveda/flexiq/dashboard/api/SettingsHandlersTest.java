package org.byteveda.flexiq.dashboard.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.support.DashboardError;
import org.junit.jupiter.api.Test;

class SettingsHandlersTest {

    private final InMemorySettings settings = new InMemorySettings();
    private final SettingsHandlers handlers = new SettingsHandlers(settings);

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    @Test
    void putAndGetRoundTrip() {
        assertEquals("v", asMap(handlers.put("k", Map.of("value", "v"))).get("value"));
        assertEquals("v", asMap(handlers.get("k")).get("value"));
        assertEquals("k", asMap(handlers.get("k")).get("key"));
    }

    @Test
    void nonStringValueIsJsonEncoded() {
        handlers.put("cfg", Map.of("value", Map.of("a", 1)));
        assertEquals("{\"a\":1}", asMap(handlers.get("cfg")).get("value"));
    }

    @Test
    void protectedKeysAreInvisible() {
        settings.setSetting("auth:users", "{}");
        settings.setSetting("webhooks:subscriptions", "[]");
        settings.setSetting("retention:effective:default", "{}");
        handlers.put("visible", Map.of("value", "1"));
        Map<String, Object> listed = asMap(handlers.list());
        assertTrue(listed.containsKey("visible"));
        assertTrue(!listed.containsKey("auth:users"));
        assertTrue(!listed.containsKey("webhooks:subscriptions"));
        // The published retention document is a report, not an editable row.
        assertTrue(!listed.containsKey("retention:effective:default"));
        assertNull(handlers.get("auth:users"));
        assertNull(handlers.get("retention:effective:default"));
        assertThrows(DashboardError.class, () -> handlers.put("auth:x", Map.of("value", "1")));
        assertThrows(DashboardError.class, () -> handlers.delete("webhooks:subscriptions"));
        assertThrows(DashboardError.class, () -> handlers.put("retention:effective:default", Map.of("value", "{}")));
    }

    @Test
    void deleteReportsExistence() {
        handlers.put("k", Map.of("value", "v"));
        assertEquals(true, asMap(handlers.delete("k")).get("deleted"));
        assertEquals(false, asMap(handlers.delete("k")).get("deleted"));
    }

    @Test
    void rejectsInvalidKeys() {
        assertThrows(DashboardError.class, () -> handlers.put("", Map.of("value", "v")));
        assertThrows(DashboardError.class, () -> handlers.put("a".repeat(257), Map.of("value", "v")));
        assertThrows(DashboardError.class, () -> handlers.put("bad\nkey", Map.of("value", "v")));
    }

    @Test
    void rejectsOversizedValue() {
        String big = "x".repeat(64 * 1024 + 1);
        assertThrows(DashboardError.class, () -> handlers.put("k", Map.of("value", big)));
    }
}
