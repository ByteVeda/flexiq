package org.byteveda.flexiq.internal;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.byteveda.flexiq.spi.StepSession;
import org.byteveda.flexiq.spi.WorkerControl;

/**
 * JNI-backed {@link WorkerControl} over a native worker handle. Every call holds
 * the read lock; {@code close()} takes the write lock, so it waits out in-flight
 * calls and later calls throw {@link IllegalStateException} instead of touching
 * freed native memory.
 */
public final class JniWorkerControl implements WorkerControl {
    private final long handle;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private boolean closed; // guarded by stateLock

    JniWorkerControl(long handle) {
        this.handle = handle;
    }

    private <T> T withOpenHandle(Supplier<T> nativeCall) {
        stateLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("worker control is closed");
            }
            return nativeCall.get();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void completeJob(long token, byte[] result) {
        withOpenHandle(() -> {
            NativeWorker.completeJob(handle, token, result);
            return null;
        });
    }

    @Override
    public void failJob(long token, String error, boolean retryable) {
        withOpenHandle(() -> {
            NativeWorker.failJob(handle, token, error, retryable);
            return null;
        });
    }

    @Override
    public void cancelJob(long token) {
        withOpenHandle(() -> {
            NativeWorker.cancelJob(handle, token);
            return null;
        });
    }

    @Override
    public void sleepJob(long token, long wakeAt) {
        withOpenHandle(() -> {
            NativeWorker.sleepJob(handle, token, wakeAt);
            return null;
        });
    }

    /**
     * The one control that can: it holds the native worker handle, so the owner
     * every step write is fenced on is the id <i>this</i> worker claims
     * execution under.
     */
    @Override
    public StepSession openStepSession(String jobId, int attempt) {
        return withOpenHandle(() -> new JniStepSession(NativeWorker.openStepSession(handle, jobId, attempt)));
    }

    @Override
    public void stop() {
        withOpenHandle(() -> {
            NativeWorker.stop(handle);
            return null;
        });
    }

    @Override
    public Optional<String> meshClusterInfoJson() {
        return withOpenHandle(() -> Optional.ofNullable(NativeWorker.meshClusterInfo(handle)));
    }

    /** Idempotent: frees the native worker handle exactly once, after in-flight calls drain. */
    @Override
    public void close() {
        stateLock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                NativeWorker.close(handle);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }
}
