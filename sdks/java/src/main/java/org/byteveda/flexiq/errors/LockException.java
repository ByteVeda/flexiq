package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A distributed lock operation failed or was interrupted while waiting to
 * acquire the lock.
 */
public class LockException extends FlexiQException {
    /**
     * A lock operation that failed on its own terms.
     *
     * @param message which lock, and what went wrong acquiring or releasing it
     */
    public LockException(String message) {
        super(message);
    }

    /**
     * A lock operation that failed because something underneath did.
     *
     * @param message which lock, and what went wrong acquiring or releasing it
     * @param cause the interruption or backend failure underneath
     */
    public LockException(String message, Throwable cause) {
        super(message, cause);
    }
}
