package org.byteveda.flexiq.dashboard;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.byteveda.flexiq.dashboard.store.SettingsAccess;

/** In-memory {@link SettingsAccess} for unit tests. */
public final class InMemorySettings implements SettingsAccess {
    private final Map<String, String> map = new HashMap<>();

    @Override
    public Optional<String> getSetting(String key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void setSetting(String key, String value) {
        map.put(key, value);
    }

    @Override
    public boolean setSettingIf(String key, Optional<String> expected, String value) {
        if (!Optional.ofNullable(map.get(key)).equals(expected)) {
            return false;
        }
        map.put(key, value);
        return true;
    }

    @Override
    public boolean deleteSetting(String key) {
        return map.remove(key) != null;
    }

    @Override
    public Map<String, String> listSettings() {
        return new HashMap<>(map);
    }

    /**
     * Mirrors the core list so unit tests need no native library.
     * {@code ReservedSettingPrefixesTest} fails if the two drift.
     */
    @Override
    public List<String> reservedPrefixes() {
        return List.of(
                "auth:", "contract:", "middleware:disabled:", "retention:", "flexiq.webhooks", "webhook:", "webhooks:");
    }
}
