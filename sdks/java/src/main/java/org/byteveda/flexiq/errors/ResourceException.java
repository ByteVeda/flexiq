package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A worker resource could not be built, resolved, or disposed — e.g. a factory
 * threw, an unknown resource name was requested, or {@code Resources.use} was
 * called outside a task handler.
 */
public class ResourceException extends FlexiQException {
    public ResourceException(String message) {
        super(message);
    }

    public ResourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
