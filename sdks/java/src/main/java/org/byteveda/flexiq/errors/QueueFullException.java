package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * An enqueue was rejected because the target queue reached its {@code maxPending}
 * admission cap, so no job was created. Enforced producer-side (a non-atomic
 * count-then-insert), so it applies even with no worker running.
 */
public class QueueFullException extends FlexiQException {
    /** The queue that rejected the enqueue; carried into the serialized form. */
    private final String queue;

    /** Pending count observed at rejection time; carried into the serialized form. */
    private final long pending;

    /** The configured cap; carried into the serialized form. */
    private final long cap;

    /**
     * A rejection carrying the numbers it was decided on, so a caller can back off
     * on them rather than re-reading the queue.
     *
     * @param queue the queue that rejected the enqueue
     * @param pending the pending count observed at rejection time
     * @param cap the {@code maxPending} the queue was configured with
     */
    public QueueFullException(String queue, long pending, long cap) {
        super("queue '" + queue + "' is full: " + pending + " pending >= maxPending " + cap);
        this.queue = queue;
        this.pending = pending;
        this.cap = cap;
    }

    /**
     * The queue that rejected the enqueue.
     *
     * @return the queue name
     */
    public String queue() {
        return queue;
    }

    /**
     * Pending count observed at rejection time.
     *
     * @return the count, as read producer-side before the insert was refused
     */
    public long pending() {
        return pending;
    }

    /**
     * The configured cap.
     *
     * @return the queue's {@code maxPending}
     */
    public long cap() {
        return cap;
    }
}
