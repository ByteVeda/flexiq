package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A settings document kept changing under a conditional write.
 *
 * <p>Thrown after the retry bound is exhausted — far past what admin-frequency
 * contention produces, so this reports a fault rather than a busy moment.
 */
public class SettingConflictException extends FlexiQException {

    /** The settings key that could not be written; carried into the serialized form. */
    private final String key;

    /**
     * A conditional write that lost its race past the retry bound.
     *
     * @param key the settings document that kept changing
     */
    public SettingConflictException(String key) {
        super("setting '" + key + "' kept changing under a conditional write");
        this.key = key;
    }

    /**
     * The settings key that could not be written.
     *
     * @return the document key
     */
    public String key() {
        return key;
    }
}
