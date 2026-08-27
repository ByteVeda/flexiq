package org.byteveda.flexiq.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.spi.StepDecision;
import org.byteveda.flexiq.spi.StepSession;
import org.byteveda.flexiq.spi.WorkerBridge;
import org.byteveda.flexiq.spi.WorkerControl;
import org.byteveda.flexiq.steps.StepSupersededError;
import org.junit.jupiter.api.Test;

/**
 * The {@code (owner, attempt)} fence on the harness's step store.
 *
 * <p>Driven through a bare {@link WorkerBridge} rather than through a task body,
 * because the rule under test is about <b>which worker</b> may write — and a
 * task body has no way to be the wrong one. Two workers on one backend, one job,
 * and the session opened on the control that does not hold the claim.
 *
 * <p>The behaviour a user sees is asserted in the parity suite, against a real
 * worker as well as this one. What is here is the half no task body can reach.
 */
class InMemoryStepFenceTest {

    private static final byte[] RESULT = "receipt".getBytes(StandardCharsets.UTF_8);

    /** Captures one dispatch and parks until the test lets it go. */
    private static final class CapturingBridge implements WorkerBridge {
        final CountDownLatch dispatched = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<String> jobId = new AtomicReference<>();
        final AtomicInteger attempt = new AtomicInteger(-1);

        @Override
        public void onJob(
                long token, String job, String taskName, byte[] payload, String metadataJson, String disabledJson) {
            onJob(token, job, taskName, payload, metadataJson, disabledJson, 0);
        }

        @Override
        public void onJob(
                long token,
                String job,
                String taskName,
                byte[] payload,
                String metadataJson,
                String disabledJson,
                int dispatchedAttempt) {
            jobId.set(job);
            attempt.set(dispatchedAttempt);
            dispatched.countDown();
            try {
                // Hold the claim open, so the fence is asked about a job that is
                // genuinely running under this worker.
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onOutcome(
                String kind,
                String job,
                String taskName,
                String error,
                int retryCount,
                boolean timedOut,
                long wallTimeNs) {}
    }

    @Test
    void aStepCommittedByAWorkerThatDoesNotHoldTheClaimIsRefused() throws Exception {
        InMemoryQueueBackend backend = new InMemoryQueueBackend();
        backend.enqueue("charge", new byte[0], "{\"queue\":\"claimed\"}");

        CapturingBridge claimant = new CapturingBridge();
        try (WorkerControl owner = backend.startWorker(claimant, "{\"queues\":[\"claimed\"]}");
                // A second worker on a queue with nothing in it: it never claims,
                // so its control is the wrong one to commit through.
                WorkerControl bystander = backend.startWorker(new CapturingBridge(), "{\"queues\":[\"idle\"]}")) {

            assertTrue(claimant.dispatched.await(10, TimeUnit.SECONDS), "the job should be dispatched");
            String jobId = claimant.jobId.get();
            assertNotNull(jobId);
            assertEquals(0, claimant.attempt.get(), "a first attempt is dispatched as attempt 0");

            try (StepSession session = bystander.openStepSession(jobId, claimant.attempt.get())) {
                StepDecision decision = session.beginRun("charge", null);
                assertEquals("charge#0", decision.stepKey());

                StepSupersededError refused = assertThrows(StepSupersededError.class, () -> session.commitRun(RESULT));
                assertFalse(refused.shouldRetry(), "a superseded attempt has nothing to retry for");
            }

            // The claim holder is unaffected: nothing was written for it.
            try (StepSession live = owner.openStepSession(jobId, claimant.attempt.get())) {
                assertNotNull(live.beginRun("charge", null).stepKey());
                live.commitRun(RESULT);
            }

            claimant.release.countDown();
        }
    }

    @Test
    void aStepCommittedUnderASupersededAttemptIsRefused() throws Exception {
        InMemoryQueueBackend backend = new InMemoryQueueBackend();
        backend.enqueue("charge", new byte[0], "{\"queue\":\"claimed\"}");

        CapturingBridge claimant = new CapturingBridge();
        try (WorkerControl owner = backend.startWorker(claimant, "{\"queues\":[\"claimed\"]}")) {
            assertTrue(claimant.dispatched.await(10, TimeUnit.SECONDS), "the job should be dispatched");
            String jobId = claimant.jobId.get();

            // The right worker, the wrong attempt: this is what a step write from
            // an attempt the job has already moved past looks like.
            try (StepSession stale = owner.openStepSession(jobId, claimant.attempt.get() + 1)) {
                stale.beginRun("charge", null);
                assertThrows(StepSupersededError.class, () -> stale.commitRun(RESULT));
            }

            claimant.release.countDown();
        }
    }
}
