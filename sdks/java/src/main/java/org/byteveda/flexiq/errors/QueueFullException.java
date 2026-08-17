package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * An enqueue was rejected because the target queue reached its {@code maxPending}
 * admission cap, so no job was created. Enforced producer-side (a non-atomic
 * count-then-insert), so it applies even with no worker running.
 */
public class QueueFullException extends FlexiQException {
    private final String queue;
    private final long pending;
    private final long cap;

    public QueueFullException(String queue, long pending, long cap) {
        super("queue '" + queue + "' is full: " + pending + " pending >= maxPending " + cap);
        this.queue = queue;
        this.pending = pending;
        this.cap = cap;
    }

    /** The queue that rejected the enqueue. */
    public String queue() {
        return queue;
    }

    /** Pending count observed at rejection time. */
    public long pending() {
        return pending;
    }

    /** The configured cap. */
    public long cap() {
        return cap;
    }
}
