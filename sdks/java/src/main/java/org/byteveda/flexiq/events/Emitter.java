package org.byteveda.flexiq.events;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.jspecify.annotations.Nullable;

/**
 * Dispatches {@link FlexiQEvent}s to registered listeners. Thread-safe. An
 * emitter built with a parent forwards every event upward after local dispatch,
 * so a worker's emitter feeds the owning queue's event hub.
 */
public final class Emitter {
    private static final FlexiQLogger LOG = FlexiQLogger.create("events");

    private final Map<EventName, List<Consumer<FlexiQEvent>>> listeners = new EnumMap<>(EventName.class);
    private final @Nullable Emitter parent;

    /** A standalone emitter: events dispatch to its own listeners only. */
    public Emitter() {
        this(null);
    }

    /** An emitter that forwards every event to {@code parent} (nullable) after local dispatch. */
    public Emitter(@Nullable Emitter parent) {
        this.parent = parent;
        // Pre-bind every name so the map is never structurally mutated after
        // construction — registration and dispatch then race only on the
        // CopyOnWriteArrayList, which is safe, so emit() needs no lock.
        for (EventName name : EventName.values()) {
            listeners.put(name, new CopyOnWriteArrayList<>());
        }
    }

    /**
     * Subscribe to a job outcome's {@link OutcomeEvent}s. Only valid for names
     * where {@link EventName#isJobOutcome()} holds — other events don't carry an
     * {@code OutcomeEvent}; subscribe to them via {@link #onEvent}.
     */
    public void on(EventName name, Consumer<OutcomeEvent> listener) {
        name.requireJobOutcome();
        onEvent(name, event -> listener.accept((OutcomeEvent) event));
    }

    /** Subscribe to any event by name; the listener narrows to the concrete type. */
    public void onEvent(EventName name, Consumer<FlexiQEvent> listener) {
        listenersFor(name).add(listener);
    }

    /** Listeners for {@code name}; every name is pre-bound at construction. */
    private List<Consumer<FlexiQEvent>> listenersFor(EventName name) {
        return Objects.requireNonNull(listeners.get(name), () -> name + " was not pre-bound");
    }

    /** Deliver a job outcome; equivalent to {@link #emit(FlexiQEvent)}. */
    public void emit(OutcomeEvent event) {
        emit((FlexiQEvent) event);
    }

    /**
     * Deliver {@code event} to its listeners, then forward it to the parent
     * emitter (when one exists); a throwing listener never blocks the rest.
     */
    public void emit(FlexiQEvent event) {
        for (Consumer<FlexiQEvent> listener : listenersFor(event.name())) {
            try {
                listener.accept(event);
            } catch (RuntimeException e) {
                // A listener fault must not break dispatch — but log it: this is
                // the only place a workflow-tracker failure would surface.
                LOG.warn("listener for " + event.name() + " threw", e);
            }
        }
        if (parent != null) {
            parent.emit(event);
        }
    }
}
