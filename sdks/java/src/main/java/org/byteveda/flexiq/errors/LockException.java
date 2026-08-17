package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A distributed lock operation failed or was interrupted while waiting to
 * acquire the lock.
 */
public class LockException extends FlexiQException {
    public LockException(String message) {
        super(message);
    }

    public LockException(String message, Throwable cause) {
        super(message, cause);
    }
}
