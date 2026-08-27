package org.byteveda.flexiq.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.jspecify.annotations.Nullable;

/**
 * Buffers payloads for one task and enqueues them in a single
 * {@code enqueueMany} call when the buffer reaches {@code maxBatch} or
 * {@code maxDelay} elapses since the first buffered item. Thread-safe;
 * {@link #close()} flushes what remains. Use with try-with-resources.
 *
 * @param <T> the task's payload type
 */
public final class Batcher<T> implements AutoCloseable {
    private final FlexiQ queue;
    private final Task<T> task;
    private final int maxBatch;
    private final long maxDelayNanos;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(Batcher::daemon);
    private final Object lock = new Object();
    private final List<T> buffer = new ArrayList<>();
    private @Nullable ScheduledFuture<?> pendingFlush;
    private boolean closed; // guarded by lock

    /**
     * A batcher for one task, its flush timer idle until the first payload arrives.
     *
     * @param queue where the batch is enqueued
     * @param task the task every buffered payload is enqueued for
     * @param maxBatch flush once this many payloads are buffered; must be positive
     * @param maxDelay flush this long after the first buffered payload, however few
     *     have accumulated; must be positive
     */
    public Batcher(FlexiQ queue, Task<T> task, int maxBatch, Duration maxDelay) {
        if (maxBatch <= 0) {
            throw new IllegalArgumentException("maxBatch must be > 0");
        }
        if (maxDelay == null || maxDelay.isNegative() || maxDelay.isZero()) {
            throw new IllegalArgumentException("maxDelay must be positive");
        }
        this.queue = queue;
        this.task = task;
        this.maxBatch = maxBatch;
        // Nanoseconds, not millis: toMillis() would truncate a sub-millisecond
        // delay to 0 and flush eagerly instead of honoring the requested delay.
        this.maxDelayNanos = maxDelay.toNanos();
    }

    /**
     * {@link #Batcher(FlexiQ, Task, int, Duration)} with the payload type inferred.
     *
     * @param queue where the batch is enqueued
     * @param task the task every buffered payload is enqueued for
     * @param maxBatch flush once this many payloads are buffered; must be positive
     * @param maxDelay flush this long after the first buffered payload; must be positive
     * @param <T> the task's payload type
     * @return the batcher, to be closed when the producer stops
     */
    public static <T> Batcher<T> of(FlexiQ queue, Task<T> task, int maxBatch, Duration maxDelay) {
        return new Batcher<>(queue, task, maxBatch, maxDelay);
    }

    /**
     * Buffer {@code payload}. Returns the job ids if this call triggered a flush
     * (the buffer reached {@code maxBatch}), otherwise an empty list.
     *
     * @param payload the work to enqueue with the rest of its batch
     * @return the job ids if this call flushed, otherwise an empty list
     */
    public List<String> add(T payload) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("batcher is closed");
            }
            buffer.add(payload);
            if (buffer.size() >= maxBatch) {
                return flushLocked();
            }
            scheduleFlush();
            return List.of();
        }
    }

    /**
     * Enqueue any buffered payloads now.
     *
     * @return the job ids, or an empty list when nothing was buffered
     */
    public List<String> flush() {
        synchronized (lock) {
            return flushLocked();
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            // Flush and mark closed atomically so no add() can slip in and
            // schedule a delayed flush that shutdownNow() would then cancel.
            flushLocked();
        }
        scheduler.shutdownNow();
    }

    private List<String> flushLocked() {
        if (pendingFlush != null) {
            pendingFlush.cancel(false);
            pendingFlush = null;
        }
        if (buffer.isEmpty()) {
            return List.of();
        }
        List<T> batch = new ArrayList<>(buffer);
        // Enqueue before clearing: if enqueueMany throws, the buffer keeps the
        // payloads so a delayed-flush failure doesn't silently drop them.
        List<String> ids = queue.enqueueMany(task, batch);
        buffer.clear();
        return ids;
    }

    private void scheduleFlush() {
        if (pendingFlush == null) {
            pendingFlush = scheduler.schedule(this::flush, maxDelayNanos, TimeUnit.NANOSECONDS);
        }
    }

    private static Thread daemon(Runnable runnable) {
        Thread thread = new Thread(runnable, "flexiq-batcher");
        thread.setDaemon(true);
        return thread;
    }
}
