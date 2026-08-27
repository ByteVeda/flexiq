package org.byteveda.flexiq.internal;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.byteveda.flexiq.spi.WorkerControl;
import org.jspecify.annotations.Nullable;

/**
 * JNI-backed {@link WorkerControl} over an attached-executor handle.
 *
 * <p>The mirror of {@link JniWorkerControl}, with the same locking contract:
 * every call holds the read lock, {@code close()} takes the write lock so it
 * waits out in-flight calls, and later calls throw instead of touching freed
 * native memory. {@link #awaitSession()} is the one exception — it parks for as
 * long as the session lasts, so it counts itself instead of holding the lock.
 */
public final class JniExecutorControl implements WorkerControl {
    private final long handle;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private boolean closed; // guarded by stateLock

    /** Monitor for {@link #waiting}, held only for the count. */
    private final Object waiters = new Object();

    /** Threads inside the native session wait; the handle is freed at zero. */
    private int waiting; // guarded by waiters

    /**
     * A control over one attached-executor handle.
     *
     * @param handle the executor handle from {@link NativeExecutor#attach}; this
     *     instance owns it and frees it on {@link #close()}
     */
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
    public void reportProgress(String jobId, int progress) {
        withOpenHandle(() -> {
            NativeExecutor.reportProgress(handle, jobId, progress);
            return null;
        });
    }

    @Override
    public void writeTaskLog(String jobId, String taskName, String level, String message, @Nullable String extra) {
        withOpenHandle(() -> {
            NativeExecutor.writeTaskLog(handle, jobId, taskName, level, message, extra);
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

    /**
     * Identity the scheduler announced when it accepted the attach.
     *
     * @return the scheduler's id
     */
    public String schedulerId() {
        return withOpenHandle(() -> NativeExecutor.schedulerId(handle));
    }

    /**
     * Identity this executor attached under.
     *
     * @return the id the scheduler knows this executor by
     */
    public String executorId() {
        return withOpenHandle(() -> NativeExecutor.executorId(handle));
    }

    /**
     * Peer label of the scheduler connection.
     *
     * @return the label, for logs and diagnostics
     */
    public String peer() {
        return withOpenHandle(() -> NativeExecutor.peer(handle));
    }

    /**
     * Whether the scheduler session is still open.
     *
     * @return {@code false} once the session ended or {@code stop()} was called
     */
    public boolean isRunning() {
        return withOpenHandle(() -> NativeExecutor.isRunning(handle));
    }

    /**
     * Block until the scheduler ends the session.
     *
     * <p>The read lock is released before the native wait rather than held across
     * it. {@link ReentrantReadWriteLock} blocks a new reader once a writer is
     * queued, so a parked waiter holding the lock would leave a concurrent
     * {@code stop()} stuck behind a {@code close()} that is itself waiting on the
     * park — and only {@code stop()} could have ended it. The handle stays alive
     * instead by counting waiters, which {@code close} drains before freeing.
     */
    public void awaitSession() {
        withOpenHandle(() -> {
            synchronized (waiters) {
                waiting++;
            }
            return null;
        });
        try {
            NativeExecutor.awaitSession(handle);
        } finally {
            synchronized (waiters) {
                waiting--;
                waiters.notifyAll();
            }
        }
    }

    /** Idempotent: drains and frees the native handle exactly once. */
    @Override
    public void close() {
        stateLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            stateLock.writeLock().unlock();
        }

        // Ends the session so anyone parked in `awaitSession` returns, then waits
        // them out: the handle must not be freed while a native call still holds
        // it. Interrupts are deferred rather than obeyed — leaving early would
        // free the handle under a parked thread.
        NativeExecutor.stop(handle);
        boolean interrupted = false;
        synchronized (waiters) {
            while (waiting > 0) {
                try {
                    waiters.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        NativeExecutor.close(handle);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
