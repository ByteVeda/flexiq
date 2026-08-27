package org.byteveda.flexiq.spi;

import org.jspecify.annotations.Nullable;

/**
 * One attempt's durable-step store, opened by
 * {@link WorkerControl#openStepSession(String, int)}.
 *
 * <p>The split form of the core's session: {@code beginRun} decides, the SDK
 * runs the body and encodes its result with the queue's serializer, and
 * {@code commitRun} stores exactly those bytes. Nothing here interprets them,
 * which is what lets an encrypting codec reach the step store unchanged.
 *
 * <p>Every method throws a
 * {@link org.byteveda.flexiq.steps.StepControlSignal} on failure — the class
 * carries the core's retry verdict, so nothing has to be parsed out of a
 * message. Not thread-safe by design: a session belongs to one attempt, and the
 * core refuses a second step while one is uncommitted.
 */
public interface StepSession extends AutoCloseable {

    /**
     * Decide what the step called {@code name} must do, without running it.
     *
     * @param name the step's name, unique enough to identify it in the sequence
     * @param key explicit identity, or {@code null} to identify by position
     * @return what the step must do: run, or return a memoized result
     */
    StepDecision beginRun(String name, @Nullable String key);

    /**
     * Commit the encoded result of the step {@link #beginRun} handed out.
     *
     * <p>{@code encoded} is post-serializer, post-codec: those are the bytes
     * stored, and the bytes the size caps are measured on.
     *
     * @param encoded the result, already serialized and through the codec chain
     */
    void commitRun(byte[] encoded);

    /**
     * Sleep for {@code durationMs}, ending the attempt if the deadline is ahead.
     *
     * @param durationMs how long to wait, measured from the first commit
     * @param name the sleep's name, or {@code null} to fall back to its position
     * @param key explicit identity, or {@code null} to identify by position
     * @return the deadline, and whether it had already passed
     */
    StepSleepOutcome sleepFor(long durationMs, @Nullable String name, @Nullable String key);

    /**
     * Sleep until {@code wakeAtMs}, an absolute instant in Unix milliseconds.
     *
     * @param wakeAtMs the deadline to wake at
     * @param name the sleep's name, or {@code null} to fall back to its position
     * @param key explicit identity, or {@code null} to identify by position
     * @return the deadline, and whether it had already passed
     */
    StepSleepOutcome sleepUntil(long wakeAtMs, @Nullable String name, @Nullable String key);

    /**
     * The id this durable run began under — the job's own, except across a dead-letter retry.
     *
     * @return the run key the steps are stored under
     */
    String runKey();

    /**
     * Close the attempt out, warning if the job has recorded steps this code no
     * longer runs. Never throws: the side effects already happened.
     */
    void finish();

    /** Release the session. Idempotent; never throws. */
    @Override
    void close();
}
