package org.byteveda.flexiq.steps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.spi.WorkerControl;
import org.junit.jupiter.api.Test;

/**
 * §9.4 — a process that cannot commit a step refuses rather than running it
 * un-memoized, and the deterministic input errors are permanent.
 */
class StepRefusalTest {

    /** An attached executor's shape: it implements the completion surface and nothing else. */
    private static final class NoStepControl implements WorkerControl {
        @Override
        public void completeJob(long token, byte[] result) {}

        @Override
        public void failJob(long token, String error, boolean retryable) {}

        @Override
        public void cancelJob(long token) {}

        @Override
        public void stop() {}

        @Override
        public void close() {}
    }

    private static StepContext context(WorkerControl control) {
        return new StepContext("job-1", 0, new JsonSerializer(), new StepLatch(), control::openStepSession);
    }

    @Test
    void aControlWithNoStepStoreRefusesRetryably() {
        StepContext step = context(new NoStepControl());

        StepUnavailableError refused =
                assertThrows(StepUnavailableError.class, () -> step.run("charge", String.class, () -> "never"));

        assertTrue(refused.shouldRetry(), "a heterogeneous fleet may place the next attempt where it can commit");
        assertTrue(refused.getMessage().contains("job-1"), "the refusal should name the job");
    }

    @Test
    void aRefusedStepNeverRunsItsBody() {
        StepContext step = context(new NoStepControl());
        boolean[] ran = {false};

        assertThrows(
                StepUnavailableError.class,
                () -> step.run("charge", String.class, () -> {
                    ran[0] = true;
                    return "never";
                }));

        assertFalse(ran[0], "refusing after the side effect would be worse than not refusing at all");
    }

    @Test
    void aRefusalIsCaughtByThrowableButNotByException() {
        StepContext step = context(new NoStepControl());
        boolean[] caughtAsException = {false};
        boolean[] caughtAsThrowable = {false};

        try {
            try {
                step.run("charge", String.class, () -> "never");
            } catch (Exception e) {
                // The catch this whole design exists to survive: a step signal is
                // an Error, so an ordinary handler's catch cannot swallow it.
                caughtAsException[0] = true;
            }
        } catch (Throwable t) {
            caughtAsThrowable[0] = true;
        }

        assertFalse(caughtAsException[0], "catch (Exception) must not see a step control signal");
        assertTrue(caughtAsThrowable[0], "catch (Throwable) still sees one — hence the swallow latch");
    }

    @Test
    void anEmptyStepNameIsPermanentAndNeedsNoSession() {
        StepContext step = context(new NoStepControl());

        StepError refused = assertThrows(StepError.class, () -> step.run("", String.class, () -> "never"));

        // Not the session's retryable refusal: this input is deterministic, so
        // the retry budget must not be spent reaching the same dead letter.
        assertFalse(refused.shouldRetry(), "a bad step name is just as bad next attempt");
        assertFalse(refused instanceof StepUnavailableError, "judged locally, before the session is opened");
    }

    @Test
    void namingARunThroughStepOptionsIsRefused() {
        StepContext step = context(new NoStepControl());

        StepError refused = assertThrows(
                StepError.class, () -> step.run("charge", String.class, () -> "never", StepOptions.named("charge")));

        assertFalse(refused.shouldRetry());
        assertTrue(refused.getMessage().contains("StepOptions.key"), "the message should point at the fix");
    }

    @Test
    void anUnusableSleepIsPermanentAndNeedsNoSession() {
        StepContext step = context(new NoStepControl());

        StepError negative = assertThrows(StepError.class, () -> step.sleep(Duration.ofSeconds(-1)));
        assertFalse(negative.shouldRetry());

        StepError missing = assertThrows(StepError.class, () -> step.sleepUntil(null));
        assertFalse(missing.shouldRetry());

        // A usable one gets as far as the session, and is refused there instead.
        assertThrows(
                StepUnavailableError.class, () -> step.sleepUntil(Instant.now().plusSeconds(60)));
    }

    @Test
    void theIdempotencyKeyIsOnlyReadableInsideAStepBody() {
        StepContext step = context(new NoStepControl());

        StepError outside = assertThrows(StepError.class, step::idempotencyKey);

        assertFalse(outside.shouldRetry());
        assertTrue(outside.getMessage().contains("inside a step body"));
    }

    @Test
    void everyRefusalLatches() {
        StepLatch latch = new StepLatch();
        StepContext step =
                new StepContext("job-1", 0, new JsonSerializer(), latch, new NoStepControl()::openStepSession);

        assertThrows(StepControlSignal.class, () -> step.run("charge", String.class, () -> "never"));

        // A body that caught that refusal and returned anyway must not be able
        // to report a result it never computed.
        StepSwallowedError swallowed = assertThrows(StepSwallowedError.class, latch::check);
        assertFalse(swallowed.shouldRetry());
        assertEquals("StepSwallowedError", swallowed.getClass().getSimpleName());
    }

    @Test
    void aSwallowedSleepIsReportedAsTheSleep() {
        // Not a failure, however the body treated it: the row, the claim
        // revocation and the reschedule were committed before the signal was
        // thrown, so the attempt is over and only the sleep describes where the
        // job went. Failing it instead lands on the woken attempt's claim —
        // a sleep does not move `retry_count`, so the fence sees the same
        // `(owner, attempt)` — and dead-letters a job that is sleeping.
        StepLatch latch = new StepLatch();
        StepSleepSignal sleeping = new StepSleepSignal("nap#0", 1_700_000_000_000L);

        latch.latch(sleeping);

        assertTrue(latch.swallowed());
        assertSame(sleeping, latch.sleep());
        assertSame(sleeping, assertThrows(StepSleepSignal.class, latch::check));
    }

    @Test
    void aSleepIsNotOverwrittenByWhateverTheBodyTriggersNext() {
        // A swallowed sleep leaves the body running unclaimed, so the next step
        // it touches raises in its turn. Latching that would lose the fact that
        // the attempt is already asleep — and reporting it would be the
        // dead-letter above.
        StepLatch latch = new StepLatch();
        StepSleepSignal sleeping = new StepSleepSignal("nap#0", 1_700_000_000_000L);

        latch.latch(sleeping);
        latch.latch(new StepError("the claim is gone", false));

        assertSame(sleeping, assertThrows(StepSleepSignal.class, latch::check));
    }
}
