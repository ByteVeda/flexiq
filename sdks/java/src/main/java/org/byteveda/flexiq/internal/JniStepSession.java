package org.byteveda.flexiq.internal;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.byteveda.flexiq.spi.StepDecision;
import org.byteveda.flexiq.spi.StepSession;
import org.byteveda.flexiq.spi.StepSleepOutcome;
import org.jspecify.annotations.Nullable;

/**
 * JNI-backed {@link StepSession} over a native session handle.
 *
 * <p>Same locking contract as {@link JniWorkerControl}: every call holds the
 * read lock, {@code close()} takes the write lock so it waits out in-flight
 * calls, and later calls throw instead of touching freed native memory. A
 * session belongs to one attempt and the core refuses two steps at once, so the
 * lock is here to make the free safe, not to allow concurrency.
 */
final class JniStepSession implements StepSession {
    private final long handle;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private boolean closed; // guarded by stateLock

    JniStepSession(long handle) {
        this.handle = handle;
    }

    private <T> T withOpenHandle(Supplier<T> nativeCall) {
        stateLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("step session is closed");
            }
            return nativeCall.get();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public StepDecision beginRun(String name, @Nullable String key) {
        return withOpenHandle(() -> NativeStepSession.beginRun(handle, name, key));
    }

    @Override
    public void commitRun(byte[] encoded) {
        withOpenHandle(() -> {
            NativeStepSession.commitRun(handle, encoded);
            return null;
        });
    }

    @Override
    public StepSleepOutcome sleepFor(long durationMs, @Nullable String name, @Nullable String key) {
        return withOpenHandle(() -> NativeStepSession.sleepFor(handle, durationMs, name, key));
    }

    @Override
    public StepSleepOutcome sleepUntil(long wakeAtMs, @Nullable String name, @Nullable String key) {
        return withOpenHandle(() -> NativeStepSession.sleepUntil(handle, wakeAtMs, name, key));
    }

    @Override
    public String runKey() {
        return withOpenHandle(() -> NativeStepSession.runKey(handle));
    }

    /** Never throws: the side effects already happened, and the attempt is over. */
    @Override
    public void finish() {
        try {
            withOpenHandle(() -> {
                NativeStepSession.finish(handle);
                return null;
            });
        } catch (RuntimeException | Error e) {
            // A warning that could not be produced is not worth a failed job.
        }
    }

    /** Idempotent: frees the native session exactly once, after in-flight calls drain. */
    @Override
    public void close() {
        stateLock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                NativeStepSession.close(handle);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }
}
