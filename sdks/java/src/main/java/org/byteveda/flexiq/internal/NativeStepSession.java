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

    /** Decide what the step called {@code name} must do, without running it. */
    public static native StepDecision beginRun(long handle, String name, @Nullable String key);

    /** Store the encoded result of the step {@code beginRun} handed out. */
    public static native void commitRun(long handle, byte[] encoded);

    /** Sleep for {@code durationMs}, ending the attempt if the deadline is ahead. */
    public static native StepSleepOutcome sleepFor(
            long handle, long durationMs, @Nullable String name, @Nullable String key);

    /** Sleep until {@code wakeAtMs}, an absolute instant in Unix milliseconds. */
    public static native StepSleepOutcome sleepUntil(
            long handle, long wakeAtMs, @Nullable String name, @Nullable String key);

    /** The id this durable run began under. */
    public static native String runKey(long handle);

    /** Warn about recorded steps this code no longer runs. Never throws. */
    public static native void finish(long handle);

    /** Reclaim the session handle. */
    public static native void close(long handle);
}
