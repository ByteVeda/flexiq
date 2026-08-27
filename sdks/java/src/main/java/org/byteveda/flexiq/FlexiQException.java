package org.byteveda.flexiq;

/** Unchecked exception for FlexiQ SDK and native errors. */
public class FlexiQException extends RuntimeException {
    /**
     * An SDK failure with nothing underlying it.
     *
     * @param message what could not be done, and why
     */
    public FlexiQException(String message) {
        super(message);
    }

    /**
     * An SDK failure raised by something underneath — the native layer included.
     *
     * @param message what could not be done, and why
     * @param cause the underlying failure
     */
    public FlexiQException(String message, Throwable cause) {
        super(message, cause);
    }
}
