package org.byteveda.taskito.internal;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.byteveda.taskito.spi.WorkerControl;

/**
 * JNI-backed {@link WorkerControl} over an attached-executor handle.
 *
 * <p>The mirror of {@link JniWorkerControl}, with the same locking contract:
 * every call holds the read lock, {@code close()} takes the write lock so it
 * waits out in-flight calls, and later calls throw instead of touching freed
 * native memory.
 */
public final class JniExecutorControl implements WorkerControl {
    private final long handle;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private boolean closed; // guarded by stateLock

    public JniExecutorControl(long handle) {
        this.handle = handle;
    }

    private <T> T withOpenHandle(Supplier<T> nativeCall) {
        stateLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("executor control is closed");
            }
            return nativeCall.get();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void completeJob(long token, byte[] result) {
        withOpenHandle(() -> {
            NativeExecutor.completeJob(handle, token, result);
            return null;
        });
    }

    @Override
    public void failJob(long token, String error, boolean retryable) {
        withOpenHandle(() -> {
            NativeExecutor.failJob(handle, token, error, retryable);
            return null;
        });
    }

    @Override
    public void cancelJob(long token) {
        withOpenHandle(() -> {
            NativeExecutor.cancelJob(handle, token);
            return null;
        });
    }

    @Override
    public void stop() {
        withOpenHandle(() -> {
            NativeExecutor.stop(handle);
            return null;
        });
    }

    /** Identity the scheduler announced when it accepted the attach. */
    public String schedulerId() {
        return withOpenHandle(() -> NativeExecutor.schedulerId(handle));
    }

    /** Identity this executor attached under. */
    public String executorId() {
        return withOpenHandle(() -> NativeExecutor.executorId(handle));
    }

    /** Peer label of the scheduler connection. */
    public String peer() {
        return withOpenHandle(() -> NativeExecutor.peer(handle));
    }

    /** Whether the scheduler session is still open. */
    public boolean isRunning() {
        return withOpenHandle(() -> NativeExecutor.isRunning(handle));
    }

    /**
     * Block until the scheduler ends the session.
     *
     * <p>Holds the read lock for the whole wait, which is what keeps {@code close}
     * from freeing the handle out from under a parked thread — {@code close} takes
     * the write lock and so queues behind it.
     */
    public void awaitSession() {
        withOpenHandle(() -> {
            NativeExecutor.awaitSession(handle);
            return null;
        });
    }

    /** Idempotent: drains and frees the native handle exactly once. */
    @Override
    public void close() {
        stateLock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                NativeExecutor.close(handle);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }
}
