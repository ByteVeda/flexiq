package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * The SDK was misconfigured — e.g. a required connection URL is missing, or a
 * storage directory could not be created.
 */
public class ConfigurationException extends FlexiQException {
    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
