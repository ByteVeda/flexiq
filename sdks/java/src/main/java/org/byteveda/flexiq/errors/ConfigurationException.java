package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * The SDK was misconfigured — e.g. a required connection URL is missing, or a
 * storage directory could not be created.
 */
public class ConfigurationException extends FlexiQException {
    /**
     * A misconfiguration the SDK spotted on its own.
     *
     * @param message which setting is missing, and what it should hold
     */
    public ConfigurationException(String message) {
        super(message);
    }

    /**
     * A misconfiguration surfaced by an underlying failure.
     *
     * @param message which setting is missing, and what it should hold
     * @param cause the I/O or parse failure that revealed it
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
