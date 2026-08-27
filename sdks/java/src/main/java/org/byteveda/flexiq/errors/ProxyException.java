package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A non-serializable value could not be turned into a {@code ProxyRef}, or a ref
 * could not be reconstructed — no handler, a signature mismatch, or a value
 * outside an allowlist.
 */
public class ProxyException extends FlexiQException {
    /**
     * A proxy operation the registry refused.
     *
     * @param message which value or ref could not be handled, and what was missing
     */
    public ProxyException(String message) {
        super(message);
    }

    /**
     * A proxy operation that failed because something underneath did.
     *
     * @param message which value or ref could not be handled, and what was missing
     * @param cause the handler or signing failure underneath
     */
    public ProxyException(String message, Throwable cause) {
        super(message, cause);
    }
}
