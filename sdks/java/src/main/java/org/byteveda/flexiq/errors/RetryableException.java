package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * Thrown by a task handler to mark the failure as transient: the job retries on
 * the task's backoff curve until its budget is spent. Overrides the task's
 * {@code retryOn} predicate, so a handler can retry one failure a whitelist
 * would otherwise dead-letter.
 *
 * <p>Retrying is already the default — reach for this only to override a
 * predicate, or to say so explicitly at the throw site.
 */
public class RetryableException extends FlexiQException {
    /**
     * A transient failure, described in words.
     *
     * @param message why the next attempt could plausibly get further
     */
    public RetryableException(String message) {
        super(message);
    }

    /**
     * A transient failure wrapping the exception that caused it.
     *
     * @param message why the next attempt could plausibly get further
     * @param cause the failure being marked transient
     */
    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
