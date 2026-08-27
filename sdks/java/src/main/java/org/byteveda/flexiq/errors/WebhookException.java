package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A webhook could not be stored, loaded, signed, or its payload encoded.
 */
public class WebhookException extends FlexiQException {
    /**
     * A webhook operation the SDK refused.
     *
     * @param message which hook or delivery step failed, and why
     */
    public WebhookException(String message) {
        super(message);
    }

    /**
     * A webhook operation that failed because something underneath did.
     *
     * @param message which hook or delivery step failed, and why
     * @param cause the transport, storage or encoding failure underneath
     */
    public WebhookException(String message, Throwable cause) {
        super(message, cause);
    }
}
