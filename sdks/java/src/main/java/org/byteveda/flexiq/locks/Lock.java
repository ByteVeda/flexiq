package org.byteveda.flexiq.locks;

import java.time.Duration;
import java.util.UUID;
import org.byteveda.flexiq.errors.LockException;
import org.byteveda.flexiq.spi.QueueBackend;

/**
 * A distributed, TTL-bounded advisory lock. Owner-scoped to a per-instance id, so
 * only this {@code Lock} can release or extend what it acquired. {@link #close()}
 * releases it (use try-with-resources).
 */
public final class Lock implements AutoCloseable {
    private final QueueBackend backend;
    private final String name;
    private final String ownerId = UUID.randomUUID().toString();
    private final long ttlMs;
    private boolean held;

    public Lock(QueueBackend backend, String name, long ttlMs) {
        this.backend = backend;
        this.name = name;
        this.ttlMs = ttlMs;
    }

    /** Try to acquire; false if another owner holds a live lock. */
    public boolean acquire() {
        held = backend.acquireLock(name, ownerId, ttlMs);
        return held;
    }

    /** Acquire, retrying every 50ms until obtained or {@code timeout} elapses. */
    public boolean tryAcquire(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!acquire()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LockException("interrupted while acquiring lock '" + name + "'", e);
            }
        }
        return true;
    }

    /** Extend the TTL if still held; false otherwise. A failed extend means the
     * lock was lost (expired or stolen), so {@link #isHeld()} flips to false. */
    public boolean extend(long ttlMs) {
        held = backend.extendLock(name, ownerId, ttlMs);
        return held;
    }

    public boolean isHeld() {
        return held;
    }

    public void release() {
        if (held) {
            backend.releaseLock(name, ownerId);
            held = false;
        }
    }

    @Override
    public void close() {
        release();
    }
}
