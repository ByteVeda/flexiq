package org.byteveda.flexiq.resources;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.byteveda.flexiq.errors.ResourceException;

/**
 * A bounded checkout/return pool backing {@link ResourceScope#POOLED} resources
 * (part of the cross-SDK contract for pooled resources). A semaphore caps
 * concurrent checkouts at {@link PoolConfig#poolSize()}; released instances park
 * on an idle deque and are reused unless {@link PoolConfig#maxLifetime()} has
 * elapsed, in which case they are disposed and a fresh instance is built. The
 * factory always runs on the acquiring thread (so per-thread bookkeeping such as
 * dependency-cycle detection keeps working). Thread-safe.
 */
public final class ResourcePool {
    private static final Logger LOG = System.getLogger(ResourcePool.class.getName());
    /** Sentinel distinguishing "no idle instance" from a factory that returned {@code null}. */
    private static final Object NO_IDLE = new Object();

    private final String name;
    private final PoolConfig config;
    private final Supplier<Object> factory;
    private final Consumer<Object> dispose;

    private final Semaphore capacity;
    private final Deque<IdleInstance> idle = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int active; // guarded by lock
    private long totalAcquisitions; // guarded by lock
    private long totalTimeouts; // guarded by lock
    private boolean closed; // guarded by lock

    /** An instance parked in the pool, stamped so {@code maxLifetime} can expire it. */
    private record IdleInstance(Object instance, long createdAtNanos) {
        static IdleInstance of(Object instance) {
            return new IdleInstance(instance, System.nanoTime());
        }
    }

    /**
     * A point-in-time snapshot of the pool.
     *
     * @param size the configured capacity
     * @param active instances currently checked out
     * @param idle instances parked and ready for reuse
     * @param totalAcquisitions successful checkouts so far
     * @param totalTimeouts checkouts that gave up waiting for capacity
     */
    public record Stats(int size, int active, int idle, long totalAcquisitions, long totalTimeouts) {}

    /**
     * @param name the resource name (used in error messages and logs)
     * @param config pool sizing and timing
     * @param factory builds one instance; invoked on the acquiring thread
     * @param dispose tears one instance down, or {@code null} for none
     */
    public ResourcePool(String name, PoolConfig config, Supplier<Object> factory, Consumer<Object> dispose) {
        this.name = name;
        this.config = config;
        this.factory = factory;
        this.dispose = dispose;
        this.capacity = new Semaphore(config.poolSize());
    }

    /** Build {@link PoolConfig#poolMin()} instances upfront, best-effort: log and stop on the first failure. */
    public void prewarm() {
        for (int i = 0; i < config.poolMin(); i++) {
            Object instance;
            try {
                instance = factory.get();
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "failed to prewarm resource '" + name + "'", e);
                return;
            }
            lock.lock();
            try {
                if (closed) {
                    // A shutdown raced the prewarm — don't park instances on a
                    // drained pool; dispose the fresh build and stop.
                    disposeQuietly(instance);
                    return;
                }
                idle.addLast(IdleInstance.of(instance));
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Check an instance out, blocking up to {@link PoolConfig#acquireTimeout()}
     * for capacity. Reuses an idle instance when one is still within its
     * lifetime; otherwise builds a fresh one on the calling thread.
     *
     * @throws ResourceException when the pool stays exhausted past the timeout
     */
    public Object acquire() {
        awaitCapacity();
        Object reused = pollIdle();
        if (reused != NO_IDLE) {
            return reused;
        }
        // No idle instance — build one, holding the permit throughout. The active
        // count moves only after the factory succeeds, so a failing factory can
        // neither leak capacity nor underflow the counter.
        Object instance;
        try {
            instance = factory.get();
        } catch (RuntimeException | Error e) {
            capacity.release();
            throw e;
        }
        lock.lock();
        try {
            recordAcquired();
        } finally {
            lock.unlock();
        }
        return instance;
    }

    /** Return a checked-out instance: park it for reuse, or dispose it if the pool has shut down. */
    public void release(Object instance) {
        lock.lock();
        try {
            active--;
            if (closed) {
                disposeQuietly(instance);
            } else {
                idle.addLast(IdleInstance.of(instance));
            }
        } finally {
            lock.unlock();
        }
        capacity.release();
    }

    /** Dispose every idle instance and mark the pool shut down (later releases dispose immediately). */
    public void shutdown() {
        lock.lock();
        try {
            closed = true;
            while (!idle.isEmpty()) {
                disposeQuietly(idle.pollFirst().instance());
            }
        } finally {
            lock.unlock();
        }
    }

    /** A snapshot of the pool's counters. */
    public Stats stats() {
        lock.lock();
        try {
            return new Stats(config.poolSize(), active, idle.size(), totalAcquisitions, totalTimeouts);
        } finally {
            lock.unlock();
        }
    }

    /** Take a capacity permit or fail with a timeout error. */
    private void awaitCapacity() {
        boolean acquired;
        try {
            acquired = capacity.tryAcquire(config.acquireTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResourceException("interrupted while acquiring resource '" + name + "' from its pool", e);
        }
        if (!acquired) {
            lock.lock();
            try {
                totalTimeouts++;
            } finally {
                lock.unlock();
            }
            throw new ResourceException("resource '" + name + "' pool timed out after "
                    + config.acquireTimeout().toMillis() + "ms");
        }
    }

    /** Pop a live idle instance, disposing any that outlived {@code maxLifetime}; {@link #NO_IDLE} if none. */
    private Object pollIdle() {
        lock.lock();
        try {
            while (!idle.isEmpty()) {
                IdleInstance entry = idle.pollFirst();
                if (!expired(entry)) {
                    recordAcquired();
                    return entry.instance();
                }
                disposeQuietly(entry.instance());
            }
            return NO_IDLE;
        } finally {
            lock.unlock();
        }
    }

    private boolean expired(IdleInstance entry) {
        Duration lifetime = config.maxLifetime();
        return lifetime != null && System.nanoTime() - entry.createdAtNanos() >= lifetime.toNanos();
    }

    /** Update counters for a successful checkout. Caller holds {@code lock}. */
    private void recordAcquired() {
        totalAcquisitions++;
        active++;
    }

    /** Dispose one instance; a failing disposer is logged, never propagated. */
    private void disposeQuietly(Object instance) {
        if (dispose == null) {
            return;
        }
        try {
            dispose.accept(instance);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "disposing pooled resource '" + name + "' failed", e);
        }
    }
}
