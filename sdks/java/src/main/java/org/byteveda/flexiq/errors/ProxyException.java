package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A non-serializable value could not be turned into a {@code ProxyRef}, or a ref
 * could not be reconstructed — no handler, a signature mismatch, or a value
 * outside an allowlist.
 */
public class ProxyException extends FlexiQException {
    public ProxyException(String message) {
        super(message);
    }

    public ProxyException(String message, Throwable cause) {
        super(message, cause);
    }
}
