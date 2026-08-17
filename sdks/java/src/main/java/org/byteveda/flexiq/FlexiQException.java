package org.byteveda.flexiq;

/** Unchecked exception for FlexiQ SDK and native errors. */
public class FlexiQException extends RuntimeException {
    public FlexiQException(String message) {
        super(message);
    }

    public FlexiQException(String message, Throwable cause) {
        super(message, cause);
    }
}
