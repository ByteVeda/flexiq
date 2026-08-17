package org.byteveda.flexiq.dashboard.store;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.internal.NativeQueue;
import org.byteveda.flexiq.spi.ConditionalSettings;

/**
 * Narrow view of the {@code dashboard_settings} KV store — the single
 * persistence primitive behind auth, OAuth state, overrides, middleware toggles
 * and webhooks. Every backend already exposes it, so no schema changes are
 * needed. Decoupling the stores from {@link FlexiQ} keeps them unit-testable
 * against an in-memory map.
 */
public interface SettingsAccess extends ConditionalSettings {

    void setSetting(String key, String value);

    /**
     * {@inheritDoc}
     *
     * <p>Defaults to a <b>non-atomic</b> read-compare-write, so a store written
     * before this method existed keeps compiling and behaves as it did. See
     * {@link org.byteveda.flexiq.spi.QueueBackend#setSettingIf}.
     */
    @Override
    default boolean setSettingIf(String key, Optional<String> expected, String value) {
        if (!getSetting(key).equals(expected)) {
            return false;
        }
        setSetting(key, value);
        return true;
    }

    /** @return whether a row existed. */
    boolean deleteSetting(String key);

    /** All settings; callers filter by key prefix. */
    Map<String, String> listSettings();

    /**
     * Key prefixes the generic settings API must treat as absent (auth state,
     * webhooks, runtime-published documents). Comes from the core so every shell
     * hides the same keys; resolved on call, not on class load, so an in-memory
     * store can answer without the native library.
     */
    default List<String> reservedPrefixes() {
        return List.of(NativeQueue.reservedSettingPrefixes());
    }

    static SettingsAccess of(FlexiQ queue) {
        return new SettingsAccess() {
            @Override
            public Optional<String> getSetting(String key) {
                return queue.getSetting(key);
            }

            @Override
            public void setSetting(String key, String value) {
                queue.setSetting(key, value);
            }

            @Override
            public boolean setSettingIf(String key, Optional<String> expected, String value) {
                return queue.setSettingIf(key, expected, value);
            }

            @Override
            public boolean deleteSetting(String key) {
                return queue.deleteSetting(key);
            }

            @Override
            public Map<String, String> listSettings() {
                return queue.listSettings();
            }
        };
    }
}
