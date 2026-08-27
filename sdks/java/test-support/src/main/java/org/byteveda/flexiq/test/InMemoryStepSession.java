package org.byteveda.flexiq.test;

import java.util.List;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.spi.StepDecision;
import org.byteveda.flexiq.spi.StepSession;
import org.byteveda.flexiq.spi.StepSleepOutcome;
import org.byteveda.flexiq.steps.StepError;
import org.byteveda.flexiq.steps.StepLimitExceededError;
import org.jspecify.annotations.Nullable;

/**
 * One attempt's durable steps against {@link InMemoryQueueBackend}.
 *
 * <p>The rules of {@link StepSequence} driven against the backend's step map,
 * fenced on the {@code (owner, attempt)} this worker won its claim under. It
 * never looks inside a step's bytes: encoding is the queue's serializer and
 * codec chain, so it commits exactly what it is handed and returns exactly what
 * it read.
 *
 * <p>The snapshot is read <b>once</b>, here, and every later question is
 * answered from it — the same read-once shape a real session has, and the reason
 * a divergence surfaces at the step that changed rather than at the end of the
 * attempt.
 */
final class InMemoryStepSession implements StepSession {

    private static final FlexiQLogger LOG = FlexiQLogger.create("worker");

    /** Largest encoded result one step may commit. The core's default. */
    private static final int MAX_STEP_BYTES = 256 * 1024;

    /** Largest total across every committed step of one job. The core's default. */
    private static final int MAX_TOTAL_BYTES = 4 * 1024 * 1024;

    /** Most steps one job may commit. The core's default. */
    private static final int MAX_STEPS = 1_000;

    private final InMemoryQueueBackend backend;
    private final String jobId;
    private final String owner;
    private final int attempt;

    /** The id this durable run began under — the job's own, except across a dead-letter retry. */
    private final String runKey;

    private final StepSequence sequence;

    /**
     * The step {@code beginRun} issued, waiting for its bytes. Cleared by
     * {@code commitRun} whether or not the write succeeded: a refused commit has
     * already failed the attempt, and keeping the token would let a later call
     * store bytes against a step the sequence has moved past.
     */
    private StepSequence.@Nullable PendingStep pending;

    private boolean closed;

    InMemoryStepSession(InMemoryQueueBackend backend, String jobId, String owner, int attempt) {
        this.backend = backend;
        this.jobId = jobId;
        this.owner = owner;
        this.attempt = attempt;
        this.runKey = backend.runKeyOf(jobId);
        this.sequence = new StepSequence(jobId, backend.loadSteps(jobId));
    }

    @Override
    public StepDecision beginRun(String name, @Nullable String key) {
        checkOpen();
        StepSequence.RunOutcome outcome = sequence.beginRun(name, key);
        pending = outcome.pending();
        return new StepDecision(outcome.memoized(), outcome.stepKey(), idempotencyKey(outcome.stepKey()));
    }

    @Override
    public void commitRun(byte[] encoded) {
        checkOpen();
        StepSequence.PendingStep step = pending;
        if (step == null) {
            throw new StepError("job " + jobId + " committed a step result with no step outstanding", false);
        }
        pending = null;
        checkCaps(step.stepKey(), encoded.length, 1);
        backend.commitStep(jobId, owner, attempt, StepRecord.run(step.seq(), step.stepKey(), encoded.clone()));
        // Only after the store has it: a failed commit leaves the sequence where
        // it was, and the attempt ends there anyway.
        sequence.commit(step, encoded.length);
    }

    @Override
    public StepSleepOutcome sleepFor(long durationMs, @Nullable String name, @Nullable String key) {
        // The clock is read once, here. A binding that recomputed `now + duration`
        // on each replay would push the deadline a full duration further out every
        // time the job crashed into it — a sleep that outlives the job, produced
        // by the recovery path itself.
        long now = System.currentTimeMillis();
        return sleepAt(name, key, saturatingAdd(now, durationMs), now);
    }

    @Override
    public StepSleepOutcome sleepUntil(long wakeAtMs, @Nullable String name, @Nullable String key) {
        return sleepAt(name, key, wakeAtMs, System.currentTimeMillis());
    }

    @Override
    public String runKey() {
        checkOpen();
        return runKey;
    }

    @Override
    public void finish() {
        if (closed) {
            return;
        }
        List<String> orphaned = sequence.orphanedTail();
        if (orphaned.isEmpty()) {
            return;
        }
        // A warning, never a failure: those side effects already happened, and the
        // shortened code has no use for their values.
        LOG.warn("job " + jobId + " has " + orphaned.size() + " recorded step(s) its code no longer runs: ["
                + String.join(", ", orphaned) + "]. Recorded: [" + String.join(", ", sequence.recordedKeys())
                + "]; this attempt ran: [" + String.join(", ", sequence.issuedKeys()) + "].");
    }

    @Override
    public void close() {
        closed = true;
    }

    // -------------------------------------------------------------- private

    /** Both sleeps, with the clock read exactly once by the caller. */
    private StepSleepOutcome sleepAt(@Nullable String name, @Nullable String key, long wakeAt, long now) {
        checkOpen();
        StepSequence.SleepOutcome outcome = sequence.beginSleep(name, key, now);
        if (outcome.elapsed()) {
            return new StepSleepOutcome(true, outcome.stepKey(), outcome.wakeAt());
        }
        StepSequence.PendingStep step = outcome.pending();
        if (step == null) {
            throw new StepError("job " + jobId + " asked to sleep but was issued no step", false);
        }
        // Only new ground can be refused by the step-count cap: a resume found the
        // deadline already stored and writes nothing to count.
        if (outcome.fresh()) {
            checkCaps(step.stepKey(), 0, 1);
        }
        InMemoryQueueBackend.SleepCommit commit =
                backend.commitSleep(jobId, owner, attempt, StepRecord.sleep(step.seq(), step.stepKey(), wakeAt));
        sequence.commitSleep(step, commit.fresh());
        // The deadline the store settled on, never the candidate: on a replay they
        // are different numbers and the job was rescheduled to the store's.
        return new StepSleepOutcome(false, step.stepKey(), commit.wakeAt());
    }

    /** {@code {runKey}:{stepKey}} — the key to hand the downstream service. */
    private String idempotencyKey(String stepKey) {
        return runKey + ":" + stepKey;
    }

    /**
     * Refuse an over-cap commit before it lands, so the error names the step and
     * the number that failed.
     */
    private void checkCaps(String stepKey, int encodedLength, int rows) {
        if (encodedLength > MAX_STEP_BYTES) {
            throw overCap(stepKey, "step bytes", encodedLength, MAX_STEP_BYTES);
        }
        int steps = sequence.committedSteps() + rows;
        if (steps > MAX_STEPS) {
            throw overCap(stepKey, "step count", steps, MAX_STEPS);
        }
        int bytes = sequence.committedBytes() + encodedLength;
        if (bytes > MAX_TOTAL_BYTES) {
            throw overCap(stepKey, "total bytes", bytes, MAX_TOTAL_BYTES);
        }
    }

    private static StepLimitExceededError overCap(String stepKey, String limit, int actual, int allowed) {
        return new StepLimitExceededError(
                "step '" + stepKey + "' exceeds the " + limit + " limit: " + actual + " > " + allowed);
    }

    private void checkOpen() {
        if (closed) {
            throw new StepError("the step session for job " + jobId + " is closed", false);
        }
    }

    private static long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            return right > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
    }
}
