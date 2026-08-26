package org.byteveda.flexiq.worker;

import java.util.function.Predicate;
import org.byteveda.flexiq.errors.NonRetryableException;
import org.byteveda.flexiq.errors.RetryableException;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.steps.StepControlSignal;
import org.jspecify.annotations.Nullable;

/**
 * Classifies a failed attempt as retryable or permanent. A durable-step signal
 * outranks everything; below that, a typed signal thrown by the handler
 * ({@link RetryableException} / {@link NonRetryableException}) wins over the
 * task's {@code retryOn} predicate; with neither, every failure retries.
 */
final class RetryDecision {
    private static final FlexiQLogger LOG = FlexiQLogger.create("worker");
    /** Bounds the cause walk so a self-referential chain can't spin the worker thread. */
    private static final int MAX_CAUSE_DEPTH = 16;

    private RetryDecision() {}

    /** Whether {@code error} should be retried under {@code retryOn} ({@code null} = no predicate). */
    static boolean isRetryable(@Nullable Predicate<Throwable> retryOn, Throwable error) {
        // The core's step verdict first, and it is final. A divergence, a size
        // cap or a lost claim will be exactly as wrong next attempt, and the
        // task's retryOn predicate has an opinion about the task's own
        // exceptions — nothing useful to say about the step machinery's.
        StepControlSignal step = stepSignal(error);
        if (step != null) {
            return step.shouldRetry();
        }
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
     * The durable-step signal in {@code error}'s cause chain, or {@code null}.
     *
     * <p>Walked rather than tested at the top, because a body that caught a
     * signal and rethrew it wrapped must not thereby launder a permanent failure
     * into a retryable one.
     */
    private static @Nullable StepControlSignal stepSignal(Throwable error) {
        Throwable cause = error;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (cause instanceof StepControlSignal) {
                return (StepControlSignal) cause;
            }
            cause = cause.getCause();
        }
        return null;
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
