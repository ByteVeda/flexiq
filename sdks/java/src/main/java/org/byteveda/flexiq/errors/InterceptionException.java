package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/** An interceptor rejected an enqueue, so no job was created. */
public class InterceptionException extends FlexiQException {
    /**
     * An interceptor that refused an enqueue.
     *
     * @param message which interceptor rejected the enqueue, and why
     */
    public InterceptionException(String message) {
        super(message);
    }
}
