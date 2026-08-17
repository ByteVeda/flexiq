package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A webhook could not be stored, loaded, signed, or its payload encoded.
 */
public class WebhookException extends FlexiQException {
    public WebhookException(String message) {
        super(message);
    }

    public WebhookException(String message, Throwable cause) {
        super(message, cause);
    }
}
