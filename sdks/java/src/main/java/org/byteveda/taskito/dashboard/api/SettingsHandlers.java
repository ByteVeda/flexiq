package org.byteveda.taskito.dashboard.api;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.byteveda.taskito.dashboard.store.SettingsAccess;
import org.byteveda.taskito.dashboard.support.DashboardError;
import org.byteveda.taskito.dashboard.support.Json;
import org.jspecify.annotations.Nullable;

/**
 * Generic settings KV API. Keys under the core's reserved prefixes ({@code auth:},
 * {@code webhooks:}, {@code retention:}, …) are treated as absent everywhere — never
 * listed, read, written, or deleted through this surface — so auth state, webhooks, and
 * published runtime documents cannot be exposed or clobbered. Keys are capped at 256
 * chars, values at 64 KiB.
 */
public final class SettingsHandlers {
    static final int MAX_KEY_LENGTH = 256;
    static final int MAX_VALUE_LENGTH = 64 * 1024;

    private final SettingsAccess settings;
    // Auth state, the webhook store, and the retention windows the cleaner
    // publishes. The store hands over the core's list, so every shell hides the
    // same keys and this class never touches the native library itself.
    private final List<String> protectedPrefixes;

    public SettingsHandlers(SettingsAccess settings) {
        this.settings = settings;
        this.protectedPrefixes = settings.reservedPrefixes();
    }

    public Object list() {
        Map<String, Object> out = new LinkedHashMap<>();
        settings.listSettings().forEach((key, value) -> {
            if (!isProtected(key)) {
                out.put(key, value);
            }
        });
        return out;
    }

    public @Nullable Object get(String key) {
        if (isProtected(key)) {
            return null; // read as absent → 404
        }
        return settings.getSetting(key).map(value -> entry(key, value)).orElse(null);
    }

    public Object put(String key, Map<String, Object> body) {
        validateKey(key);
        @Nullable Object raw = body.get("value");
        String value = raw instanceof String s ? s : Json.toString(raw);
        if (value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_VALUE_LENGTH) {
            throw DashboardError.badRequest("value too large");
        }
        settings.setSetting(key, value);
        return entry(key, value);
    }

    public Object delete(String key) {
        if (isProtected(key)) {
            throw DashboardError.notFound("not found");
        }
        return Map.of("deleted", settings.deleteSetting(key));
    }

    private static Map<String, Object> entry(String key, String value) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", key);
        m.put("value", value);
        return m;
    }

    private boolean isProtected(String key) {
        return protectedPrefixes.stream().anyMatch(key::startsWith);
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH) {
            throw DashboardError.badRequest("invalid setting key");
        }
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c < 32 || c == 127) {
                throw DashboardError.badRequest("invalid setting key");
            }
        }
        if (isProtected(key)) {
            throw DashboardError.badRequest("setting key is reserved");
        }
    }
}
