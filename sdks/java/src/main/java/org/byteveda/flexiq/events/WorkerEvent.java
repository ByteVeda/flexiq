package org.byteveda.flexiq.events;

import java.util.List;

/**
 * A worker lifecycle transition — one of {@link EventName#WORKER_STARTED},
 * {@link EventName#WORKER_ONLINE}, {@link EventName#WORKER_STOPPED},
 * {@link EventName#WORKER_OFFLINE}, or {@link EventName#WORKER_UNHEALTHY}.
 *
 * @param name which lifecycle transition occurred
 * @param queues the queues the worker serves; empty when it serves all queues
 */
public record WorkerEvent(EventName name, List<String> queues) implements FlexiQEvent {

    /** Validates {@code name} is a worker lifecycle constant and defensively copies {@code queues}. */
    public WorkerEvent {
        switch (name) {
            case WORKER_STARTED, WORKER_ONLINE, WORKER_STOPPED, WORKER_OFFLINE, WORKER_UNHEALTHY -> {}
            default -> throw new IllegalArgumentException(name + " is not a worker lifecycle event");
        }
        queues = List.copyOf(queues);
    }
}
