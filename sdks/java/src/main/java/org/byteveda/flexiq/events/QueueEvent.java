package org.byteveda.flexiq.events;

/**
 * A named queue was paused or resumed — {@link EventName#QUEUE_PAUSED} or
 * {@link EventName#QUEUE_RESUMED}.
 *
 * @param name which transition occurred
 * @param queue the affected queue
 */
public record QueueEvent(EventName name, String queue) implements FlexiQEvent {

    /** Validates {@code name} is a queue pause/resume constant. */
    public QueueEvent {
        if (name != EventName.QUEUE_PAUSED && name != EventName.QUEUE_RESUMED) {
            throw new IllegalArgumentException(name + " is not a queue event");
        }
    }
}
