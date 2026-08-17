package org.byteveda.flexiq.worker;

import java.util.function.Predicate;
import org.byteveda.flexiq.errors.NonRetryableException;
import org.byteveda.flexiq.errors.RetryableException;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.jspecify.annotations.Nullable;

/**
 * Classifies a failed attempt as retryable or permanent. A typed signal thrown
 * by the handler ({@link RetryableException} / {@link NonRetryableException})
 * wins over the task's {@code retryOn} predicate; with neither, every failure
 * retries.
 */
final class RetryDecision {
    private static final FlexiQLogger LOG = FlexiQLogger.create("worker");
    /** Bounds the cause walk so a self-referential chain can't spin the worker thread. */
    private static final int MAX_CAUSE_DEPTH = 16;

    private RetryDecision() {}

    /** Whether {@code error} should be retried under {@code retryOn} ({@code null} = no predicate). */
    static boolean isRetryable(@Nullable Predicate<Throwable> retryOn, Throwable error) {
        Boolean signalled = signalledIntent(error);
        if (signalled != null) {
            return signalled;
        }
        if (retryOn == null) {
            return true;
        }
        try {
            return retryOn.test(error);
        } catch (RuntimeException e) {
            // A broken classifier must not silently turn transient failures into dead letters.
            LOG.warn("retryOn predicate threw; retrying the failure", e);
            return true;
        }
    }

    /**
     * The handler's explicit retry intent, or {@code null} when it threw neither
     * typed exception. Walks the cause chain so a signal wrapped by framework
     * code still counts; the outermost signal wins.
     */
    private static @Nullable Boolean signalledIntent(Throwable error) {
        Throwable cause = error;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (cause instanceof NonRetryableException) {
                return false;
            }
            if (cause instanceof RetryableException) {
                return true;
            }
            cause = cause.getCause();
        }
        return null;
    }
}
