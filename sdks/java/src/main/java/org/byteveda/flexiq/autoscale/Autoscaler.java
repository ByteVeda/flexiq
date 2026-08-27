package org.byteveda.flexiq.autoscale;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Periodically resizes a worker's handler pool to {@code ceil(depth /
 * tasksPerWorker)} clamped to {@code [minWorkers, maxWorkers]}, where depth is
 * the queue's outstanding work. Growing raises the pool ceiling before its core;
 * shrinking lowers the core and lets idle threads time out — running handlers are
 * never interrupted.
 */
public final class Autoscaler implements AutoCloseable {
    private static final Logger LOG = System.getLogger(Autoscaler.class.getName());

    private final ThreadPoolExecutor pool;
    private final LongSupplier depth;
    private final AutoscaleOptions options;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(Autoscaler::daemon);

    /**
     * An autoscaler over one pool; idle here until {@link #start()}.
     *
     * @param pool the handler pool to resize; its core threads are made to time out
     *     so the pool actually shrinks
     * @param depth reads the queue's outstanding work, called once per tick
     * @param options the bounds and cadence
     */
    public Autoscaler(ThreadPoolExecutor pool, LongSupplier depth, AutoscaleOptions options) {
        this.pool = pool;
        this.depth = depth;
        this.options = options;
        // Let core threads retire so the pool actually shrinks when idle.
        pool.allowCoreThreadTimeOut(true);
    }

    /** Begin periodic resizing. */
    public void start() {
        long ms = options.interval().toMillis();
        scheduler.scheduleAtFixedRate(this::tickSafely, ms, ms, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    /**
     * The target pool size for {@code depth} under {@code options}.
     *
     * @param depth the queue's outstanding work
     * @param options the bounds and the per-worker target
     * @return {@code ceil(depth / tasksPerWorker)} clamped to the configured range
     */
    public static int desiredSize(long depth, AutoscaleOptions options) {
        long perWorker = options.tasksPerWorker();
        long target = (depth + perWorker - 1) / perWorker; // ceil(depth / perWorker)
        return (int) Math.max(options.minWorkers(), Math.min(options.maxWorkers(), target));
    }

    private void tickSafely() {
        try {
            tick();
        } catch (Throwable e) {
            // scheduleAtFixedRate cancels all future runs if a task escapes with
            // any Throwable — catch everything and log so the loop survives a bad tick.
            LOG.log(Level.WARNING, "autoscaler tick failed; retrying next interval", e);
        }
    }

    /** Read depth once and resize the pool. Package-visible for tests. */
    void tick() {
        resize(desiredSize(depth.getAsLong(), options));
    }

    private void resize(int target) {
        // setCorePoolSize fails if it exceeds the max, so order the two writes.
        if (target > pool.getMaximumPoolSize()) {
            pool.setMaximumPoolSize(target);
            pool.setCorePoolSize(target);
        } else {
            pool.setCorePoolSize(target);
            pool.setMaximumPoolSize(target);
        }
    }

    private static Thread daemon(Runnable runnable) {
        Thread thread = new Thread(runnable, "flexiq-autoscaler");
        thread.setDaemon(true);
        return thread;
    }
}
