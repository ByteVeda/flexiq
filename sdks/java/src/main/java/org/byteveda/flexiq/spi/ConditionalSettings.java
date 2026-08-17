package org.byteveda.flexiq.spi;

import java.util.Optional;

/**
 * The settings key/value surface a lost-update-free read-modify-write needs.
 *
 * <p>The dashboard feature stores keep a whole JSON document under one settings
 * key, so a plain read-then-write drops a concurrent edit wholesale. Anything
 * that edits one goes through {@code SettingsDocument.update}, which needs
 * exactly these two operations and nothing else.
 */
public interface ConditionalSettings {

    /** The value for {@code key}, or empty when unset. */
    Optional<String> getSetting(String key);

    /**
     * Writes {@code key} only if it still holds {@code expected}, where an empty
     * {@code expected} means the key must be unset.
     *
     * @return false when another writer got there first, so a read-modify-write
     *     caller can re-read and retry instead of overwriting an unseen edit.
     */
    boolean setSettingIf(String key, Optional<String> expected, String value);
}
