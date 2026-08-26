package org.byteveda.flexiq.steps;

import com.fasterxml.jackson.core.type.TypeReference;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.byteveda.flexiq.serialization.Serializer;
import org.byteveda.flexiq.spi.StepDecision;
import org.byteveda.flexiq.spi.StepSession;
import org.byteveda.flexiq.spi.StepSleepOutcome;
import org.jspecify.annotations.Nullable;

/**
 * {@code JobContext.current().step()} — durable inline steps on the task context.
 *
 * <pre>{@code
 * JobContext ctx = JobContext.current();
 * Charge charge = ctx.step().run("charge", Charge.class,
 *         () -> stripe.charge(order, ctx.step().idempotencyKey()));
 * ctx.step().sleep(Duration.ofHours(1));
 * ctx.step().run("receipt", () -> sendReceipt(charge));
 * }</pre>
 *
 * <p>A step runs once per job, not once per attempt: its result is committed
 * and every later attempt returns the committed value instead of running it
 * again. The rules — identity, divergence, the size caps, the sleep decision —
 * live in the Rust core, which is what makes them identical across the SDKs.
 * This class is the Java side of the split the core exposes for exactly that
 * reason: the core decides, the body runs here, and the core commits the bytes
 * this SDK encoded.
 *
 * <p><b>Memoization alone is not exactly-once.</b> The process can die between a
 * payment API returning 200 and the step row committing, and the replay has no
 * record the call happened. Nothing on this side of the network closes that
 * window; only a key the other side dedupes on does, which is what
 * {@link #idempotencyKey()} is for.
 *
 * <p>A step belongs to one job. Work that must outlive a job, be distributed
 * across machines or be inspected as a graph is a workflow node, not a step.
 *
 * <p>Not thread-safe: steps are issued one at a time (the core refuses a second
 * while one is uncommitted), and a step's position is what identifies it.
 */
public final class StepContext {

    private final String jobId;
    private final int attempt;
    private final Serializer serializer;
    private final StepLatch latch;

    /** Absent where this process cannot commit a step — an attached executor. */
    private final @Nullable StepStore store;

    private @Nullable StepSession session;

    /** This step's downstream key, bound only for the length of its body. */
    private @Nullable String currentKey;

    /**
     * Built by the worker; reach it through {@code JobContext.current().step()}.
     *
     * @param jobId the running job
     * @param attempt the {@code retryCount} this job was dispatched with
     * @param serializer the <b>queue's</b> serializer, codec chain included —
     *     that is how an encrypting codec reaches the step store with no extra
     *     plumbing
     * @param latch the invocation's swallow latch, shared with the worker
     * @param store where steps commit, or {@code null} to refuse every step
     * @hidden
     */
    public StepContext(String jobId, int attempt, Serializer serializer, StepLatch latch, @Nullable StepStore store) {
        this.jobId = jobId;
        this.attempt = attempt;
        this.serializer = serializer;
        this.latch = latch;
        this.store = store;
    }

    // ------------------------------------------------------------------ run

    /**
     * Run {@code body} once for this job, or return what it returned last time.
     *
     * <p>{@code name} is required and positional: an inferred name changes
     * whenever the body is renamed or inlined, and a step whose identity moves
     * is a step whose memo answers a different question.
     *
     * <p>The first run returns exactly what {@code body} returned; a replay
     * returns that value decoded from its stored bytes, so anything the queue's
     * serializer does not round-trip exactly comes back in its decoded shape.
     * Return something the serializer preserves, or a handle to it.
     *
     * <p><b>Its signals must reach the worker.</b> A divergence, a cap violation
     * or a lost claim throws a {@link StepControlSignal}, which is an
     * {@link Error} so an ordinary {@code catch (Exception e)} around this call
     * does not see it. A {@code catch (Throwable t)} does, and swallowing one
     * fails the attempt anyway ({@link StepSwallowedError}).
     *
     * @param name this step's name, unique enough to identify it in the sequence
     * @param type the result type, for decoding a replay
     * @param body the work
     * @param <T> the step's result type
     * @return the step's result, fresh or memoized
     * @throws Exception whatever {@code body} throws
     */
    public <T> T run(String name, Class<T> type, StepBody<T> body) throws Exception {
        return run(name, type, body, StepOptions.none());
    }

    /** {@link #run(String, Class, StepBody)} with explicit identity. */
    public <T> T run(String name, Class<T> type, StepBody<T> body, StepOptions options) throws Exception {
        return runTyped(name, options, body, type);
    }

    /**
     * {@link #run(String, Class, StepBody)} for a generic result, e.g.
     * {@code new TypeReference<List<Charge>>() {}}.
     */
    public <T> T run(String name, TypeReference<T> type, StepBody<T> body) throws Exception {
        return run(name, type, body, StepOptions.none());
    }

    /** {@link #run(String, TypeReference, StepBody)} with explicit identity. */
    public <T> T run(String name, TypeReference<T> type, StepBody<T> body, StepOptions options) throws Exception {
        return runTyped(name, options, body, type.getType());
    }

    /**
     * Run {@code body} once for its side effect.
     *
     * <p>Nothing is memoized but the fact that it ran, so the replay skips it.
     *
     * @throws Exception whatever {@code body} throws
     */
    public void run(String name, StepAction body) throws Exception {
        run(name, body, StepOptions.none());
    }

    /** {@link #run(String, StepAction)} with explicit identity. */
    public void run(String name, StepAction body, StepOptions options) throws Exception {
        checkName(name, options);
        StepSession open = session();
        StepDecision decision = begin(open, name, options);
        if (decision.memoized() != null) {
            return;
        }
        bind(decision.idempotencyKey());
        try {
            body.run();
        } finally {
            currentKey = null;
        }
        // Nothing to memoize but the fact that it ran, so the row stores the
        // serializer's encoding of null and no replay ever decodes it.
        commit(open, encode(decision.stepKey(), null));
    }

    // ---------------------------------------------------------------- sleep

    /**
     * Sleep for {@code duration}, ending this attempt if the deadline is ahead.
     *
     * <p>The attempt <b>ends</b>: the claim is released and the job goes back to
     * {@code Pending} at its deadline, so a sleeping job holds no worker slot
     * and cannot be timed out while it waits. On wake the job replays from the
     * top, every earlier step is a memo hit, and this sleep returns
     * immediately.
     *
     * <p>A sleep costs no retry — the retry count, the retry budget, the circuit
     * breaker and the task metrics are all untouched.
     *
     * <p>The deadline is fixed by the <b>first</b> commit. Replaying a one-hour
     * sleep wakes at the original instant rather than an hour later each time,
     * which is what stops a crash loop from producing a sleep that outlives the
     * job.
     *
     * <p>When the deadline is still ahead this throws rather than returning —
     * the attempt is over, and anything the body does past this point runs
     * unclaimed and runs again on wake. Let it propagate.
     */
    public void sleep(Duration duration) {
        sleep(duration, StepOptions.none());
    }

    /** {@link #sleep(Duration)}, named or keyed. */
    public void sleep(Duration duration, StepOptions options) {
        long millis = millis(duration);
        StepSession open = session();
        endAttemptIfSleeping(guard(() -> open.sleepFor(millis, options.name(), options.key())));
    }

    /**
     * Sleep until an absolute instant.
     *
     * <p>Reach for this over {@link #sleep(Duration)} when the deadline means
     * something outside the job — a billing date, a market open — because an
     * absolute instant is unaffected by how many times the attempt replayed.
     */
    public void sleepUntil(Instant when) {
        sleepUntil(when, StepOptions.none());
    }

    /** {@link #sleepUntil(Instant)}, named or keyed. */
    public void sleepUntil(Instant when, StepOptions options) {
        long wakeAt = epochMillis(when);
        StepSession open = session();
        endAttemptIfSleeping(guard(() -> open.sleepUntil(wakeAt, options.name(), options.key())));
    }

    // ----------------------------------------------------------------- keys

    /**
     * The key to hand the downstream service for the step running now.
     *
     * <p>Stable across a retry, across a sleep/wake and across an operator's
     * dead-letter retry, and no serializer or codec touches it. Readable only
     * from inside a step body — outside one there is no step for it to name.
     *
     * @throws StepError when read outside a step body
     */
    public String idempotencyKey() {
        String key = currentKey;
        if (key == null) {
            throw new StepError("step.idempotencyKey() names the step that is running, so it is only readable "
                    + "inside a step body — read it from within the body passed to step.run()");
        }
        return key;
    }

    /**
     * The id this durable run began under.
     *
     * <p>The job's own id, except across an operator's dead-letter retry, which
     * mints a new job for the same run and keeps the original key so a charge is
     * not made twice.
     */
    public String runKey() {
        StepSession open = session();
        return guard(open::runKey);
    }

    // --------------------------------------------------------------- worker

    /**
     * Close the attempt out. Called by the worker; never throws.
     *
     * @hidden
     */
    public void finish() {
        StepSession open = session;
        if (open == null) {
            return;
        }
        session = null;
        try {
            open.finish();
        } finally {
            open.close();
        }
    }

    // -------------------------------------------------------------- private

    /** One step with a result: memo hit, or run and commit. */
    private <T> T runTyped(String name, StepOptions options, StepBody<T> body, Type resultType) throws Exception {
        checkName(name, options);
        StepSession open = session();
        StepDecision decision = begin(open, name, options);
        byte[] memoized = decision.memoized();
        if (memoized != null) {
            return decode(decision.stepKey(), memoized, resultType);
        }
        bind(decision.idempotencyKey());
        T value;
        try {
            value = body.get();
        } finally {
            currentKey = null;
        }
        commit(open, encode(decision.stepKey(), value));
        return value;
    }

    /**
     * Refuse what this SDK can judge on its own, before anything is opened.
     *
     * <p>Ahead of {@link #session()} deliberately: a deterministic input error
     * must not need a storage read to be reported, and must not be reported as
     * the session's retryable refusal when the same call would be just as wrong
     * next attempt.
     */
    private void checkName(String name, StepOptions options) {
        if (name == null || name.isEmpty()) {
            throw refuse("a step needs a name: step.run(\"charge\", ...)");
        }
        if (options.name() != null) {
            throw refuse("a step's name is run()'s first argument; StepOptions.named() names a sleep. "
                    + "Use StepOptions.key(...) to give step '" + name + "' an explicit identity.");
        }
    }

    /**
     * Ask the core what this step must do.
     *
     * <p>A divergence surfaces here, <i>before</i> the body runs — which is the
     * point of checking each step as it is asked for rather than at the end.
     */
    private StepDecision begin(StepSession open, String name, StepOptions options) {
        return guard(() -> open.beginRun(name, options.key()));
    }

    /** Store the bytes for the step {@link #begin} handed out. */
    private void commit(StepSession open, byte[] encoded) {
        guardVoid(() -> open.commitRun(encoded));
    }

    /** Bind this step's downstream key for the length of its body. */
    private void bind(String idempotencyKey) {
        currentKey = idempotencyKey;
    }

    /** The session for this attempt, opened once and reused. */
    private StepSession session() {
        StepSession open = session;
        if (open != null) {
            return open;
        }
        StepStore steps = store;
        if (steps == null) {
            // An attached executor has no storage and no channel to commit a
            // step on, so it refuses rather than running the step un-memoized.
            // Retryable: a heterogeneous fleet mid-rollout may put the next
            // attempt on a worker that can commit.
            throw refuseUnavailable("durable steps need a worker that reaches storage, and this task is "
                    + "running on an attached executor, which has none. Run it on an in-process worker.");
        }
        StepSession fresh = guard(() -> steps.open(jobId, attempt));
        session = fresh;
        return fresh;
    }

    /** {@link #guard(Supplier)} for a call with nothing to return. */
    private void guardVoid(Runnable body) {
        guard(() -> {
            body.run();
            return Boolean.TRUE;
        });
    }

    /**
     * Run a step-machinery call, latching the body and normalizing the failure.
     *
     * <p>Only the machinery goes through here. The step <b>body</b> is invoked
     * bare: a payment API that fails is the task failing, not a control signal,
     * and the task's own {@code retryOn} predicate should have its say about it.
     *
     * <p>Anything that is not already a control signal becomes a
     * <i>retryable</i> {@link StepError}: a native layer older than this SDK, or
     * a failure raised before the binding could classify it, is a reason to fail
     * the attempt, not to guess that it is permanent.
     */
    private <T> T guard(Supplier<T> body) {
        try {
            return body.get();
        } catch (StepControlSignal signal) {
            latch.latch();
            throw signal;
        } catch (RuntimeException e) {
            latch.latch();
            String reason = e.getMessage() == null ? e.toString() : e.getMessage();
            throw new StepError(reason, true);
        }
    }

    /**
     * Refuse a step on input this SDK can judge without asking the core.
     *
     * <p>A missing name or an unusable duration is deterministic — the replay is
     * handed the same value — so it is permanent and the retry budget must not
     * be spent on it. Latched like every other control signal: a body that
     * caught this and returned would report a result it never computed.
     */
    private StepError refuse(String message) {
        latch.latch();
        return new StepError(message, false);
    }

    private StepUnavailableError refuseUnavailable(String message) {
        latch.latch();
        return new StepUnavailableError(message);
    }

    /** Milliseconds for a sleep duration, refusing anything unusable. */
    private long millis(Duration duration) {
        if (duration == null) {
            throw refuse("step.sleep() needs a duration, e.g. Duration.ofHours(1)");
        }
        if (duration.isNegative()) {
            throw refuse("step.sleep() cannot take a negative duration: " + duration);
        }
        try {
            return duration.toMillis();
        } catch (ArithmeticException e) {
            throw refuse("step.sleep() duration does not fit in milliseconds: " + duration);
        }
    }

    private long epochMillis(Instant when) {
        if (when == null) {
            throw refuse("step.sleepUntil() needs an instant, e.g. Instant.now().plus(...)");
        }
        try {
            return when.toEpochMilli();
        } catch (ArithmeticException e) {
            throw refuse("step.sleepUntil() instant does not fit in epoch milliseconds: " + when);
        }
    }

    /**
     * Decode a memoized result.
     *
     * <p>A permanent failure when it will not decode: the stored bytes are what
     * they are, and the replay would fail the same way. It names the step,
     * because "cannot deserialize" without one is unactionable.
     */
    @SuppressWarnings("unchecked")
    private <T> T decode(String stepKey, byte[] memoized, Type resultType) {
        try {
            return (T) serializer.deserialize(memoized, resultType);
        } catch (RuntimeException e) {
            throw refuse("step '" + stepKey + "' has a stored result the queue serializer cannot decode as "
                    + resultType + ": " + e);
        }
    }

    /**
     * Encode a step result with the <b>queue's</b> serializer, not a task's.
     *
     * <p>That is how an encrypting codec reaches the step store with no extra
     * plumbing: the codec chain is already part of this serializer, so the core
     * stores ciphertext without knowing it did.
     *
     * <p>A value the serializer cannot encode is a permanent step failure — the
     * replay would produce the same value — and a control signal, because the
     * step has already run its side effect and nothing was committed.
     */
    private byte[] encode(String stepKey, @Nullable Object value) {
        try {
            return serializer.serialize(value);
        } catch (RuntimeException e) {
            throw refuse("step '" + stepKey + "' returned a value the queue serializer cannot encode: " + e);
        }
    }

    /** Unwind the body unless the deadline had already passed. */
    private void endAttemptIfSleeping(StepSleepOutcome outcome) {
        if (outcome.elapsed()) {
            return;
        }
        latch.latch();
        throw new StepSleepSignal(outcome.stepKey(), outcome.wakeAt());
    }
}
