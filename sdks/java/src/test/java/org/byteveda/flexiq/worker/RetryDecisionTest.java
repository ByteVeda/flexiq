package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;
import org.byteveda.flexiq.errors.NonRetryableException;
import org.byteveda.flexiq.errors.RetryableException;
import org.junit.jupiter.api.Test;

/** Unit-level classification rules behind a failed attempt's retry decision. */
class RetryDecisionTest {

    private static final Predicate<Throwable> REJECT_ALL = error -> false;

    @Test
    void everyFailureRetriesWithoutAPredicate() {
        assertTrue(RetryDecision.isRetryable(null, new IllegalStateException("boom")));
    }

    @Test
    void nonRetryableExceptionOverridesAnAcceptingPredicate() {
        assertFalse(RetryDecision.isRetryable(error -> true, new NonRetryableException("malformed input")));
    }

    @Test
    void retryableExceptionOverridesARejectingPredicate() {
        assertTrue(RetryDecision.isRetryable(REJECT_ALL, new RetryableException("connection reset")));
    }

    @Test
    void aWrappedSignalStillCounts() {
        Throwable wrapped = new IllegalStateException("handler failed", new NonRetryableException("bad request"));
        assertFalse(RetryDecision.isRetryable(null, wrapped));
    }

    @Test
    void theOutermostSignalWins() {
        Throwable wrapped = new RetryableException("retry the batch", new NonRetryableException("bad row"));
        assertTrue(RetryDecision.isRetryable(null, wrapped));
    }

    @Test
    void aCyclicCauseChainTerminates() {
        CyclicException error = new CyclicException();
        assertTrue(RetryDecision.isRetryable(null, error));
    }

    @Test
    void anUnsignalledFailureFallsBackToThePredicate() {
        assertFalse(RetryDecision.isRetryable(REJECT_ALL, new IllegalArgumentException("malformed input")));
    }

    @Test
    void aThrowingPredicateRetries() {
        Predicate<Throwable> broken = error -> {
            throw new IllegalStateException("classifier bug");
        };
        assertTrue(RetryDecision.isRetryable(broken, new IllegalStateException("boom")));
    }

    /** Its own cause — the JDK forbids this via initCause, an override does not. */
    private static final class CyclicException extends RuntimeException {
        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }
}
