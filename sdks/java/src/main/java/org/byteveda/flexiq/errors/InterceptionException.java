package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/** An interceptor rejected an enqueue, so no job was created. */
public class InterceptionException extends FlexiQException {
    public InterceptionException(String message) {
        super(message);
    }
}
