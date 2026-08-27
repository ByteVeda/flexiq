package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A worker resource could not be built, resolved, or disposed — e.g. a factory
 * threw, an unknown resource name was requested, or {@code Resources.use} was
 * called outside a task handler.
 */
public class ResourceException extends FlexiQException {
    /**
     * A resource operation the worker refused.
     *
     * @param message which resource, and what went wrong building or resolving it
     */
    public ResourceException(String message) {
        super(message);
    }

    /**
     * A resource operation that failed because the factory or dispose hook threw.
     *
     * @param message which resource, and what went wrong building or resolving it
     * @param cause the factory or dispose failure underneath
     */
    public ResourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
