package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * Thrown by a task handler to mark the failure as permanent: the job
 * dead-letters at once, whatever retry budget is left. For failures no amount
 * of retrying fixes — a malformed payload, a 4xx response, a rejected charge.
 *
 * <p>Overrides the task's {@code retryOn} predicate, so the throw site decides
 * even when the task classifies its failures by type.
 */
public class NonRetryableException extends FlexiQException {
    /**
     * A permanent failure, described in words.
     *
     * @param message why this failure will not resolve on a retry
     */
    public NonRetryableException(String message) {
        super(message);
    }

    /**
     * A permanent failure wrapping the exception that caused it.
     *
     * @param message why this failure will not resolve on a retry
     * @param cause the failure being marked permanent
     */
    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
