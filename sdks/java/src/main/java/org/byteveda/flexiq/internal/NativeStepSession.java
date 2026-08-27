package org.byteveda.flexiq.internal;

import org.byteveda.flexiq.spi.StepDecision;
import org.byteveda.flexiq.spi.StepSleepOutcome;
import org.jspecify.annotations.Nullable;

/**
 * JNI surface for one attempt's durable-step session.
 *
 * <p>The {@code handle} is the session pointer from
 * {@link NativeWorker#openStepSession}. Every method throws a
 * {@link org.byteveda.flexiq.steps.StepControlSignal} on failure: JNI can throw
 * any class, so the class itself carries the core's retry verdict and nothing
 * has to be parsed out of a message.
 */
public final class NativeStepSession {
    static {
        NativeLoader.load();
    }

    private NativeStepSession() {}

    /**
     * Decide what the step called {@code name} must do, without running it.
     *
     * @param handle the session handle from {@link NativeWorker#openStepSession}
     * @param name the step's name, or {@code null} to fall back to its position
     * @param key explicit identity, or {@code null} to identify by position
     * @return what the step must do: run, or return a memoized result
     */
    public static native StepDecision beginRun(long handle, String name, @Nullable String key);

    /**
     * Store the encoded result of the step {@code beginRun} handed out.
     *
     * @param handle the session handle from {@link NativeWorker#openStepSession}
     * @param encoded the result, already serialized and through the codec chain
     */
    public static native void commitRun(long handle, byte[] encoded);

    /**
     * Sleep for {@code durationMs}, ending the attempt if the deadline is ahead.
     *
     * @param handle the session handle from {@link NativeWorker#openStepSession}
     * @param durationMs how long to wait, measured from the first commit
     * @param name the step's name, or {@code null} to fall back to its position
     * @param key explicit identity, or {@code null} to identify by position
     * @return the deadline, and whether it had already passed
     */
    public static native StepSleepOutcome sleepFor(
            long handle, long durationMs, @Nullable String name, @Nullable String key);

    /**
     * Sleep until {@code wakeAtMs}, an absolute instant in Unix milliseconds.
     *
     * @param handle the session handle from {@link NativeWorker#openStepSession}
     * @param wakeAtMs the deadline to wake at
     * @param name the step's name, or {@code null} to fall back to its position
     * @param key explicit identity, or {@code null} to identify by position
     * @return the deadline, and whether it had already passed
     */
    public static native StepSleepOutcome sleepUntil(
            long handle, long wakeAtMs, @Nullable String name, @Nullable String key);

    /**
     * The id this durable run began under.
     *
     * @param handle the session handle from {@link NativeWorker#openStepSession}
     * @return the run key the steps are stored under
     */
    public static native String runKey(long handle);

    /**
     * Warn about recorded steps this code no longer runs. Never throws.
     *
     * @param handle the session handle from {@link NativeWorker#openStepSession}
     */
    public static native void finish(long handle);

    /**
     * Reclaim the session handle.
     *
     * @param handle the session handle from {@link NativeWorker#openStepSession}
     */
    public static native void close(long handle);
}
