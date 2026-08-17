package org.byteveda.flexiq.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class EmitterTest {

    @Test
    void dispatchesHeterogeneousEventsByName() {
        Emitter emitter = new Emitter();
        List<FlexiQEvent> enqueues = new CopyOnWriteArrayList<>();
        List<FlexiQEvent> pauses = new CopyOnWriteArrayList<>();
        emitter.onEvent(EventName.JOB_ENQUEUED, enqueues::add);
        emitter.onEvent(EventName.QUEUE_PAUSED, pauses::add);

        emitter.emit(new EnqueuedEvent("j1", "t", "default"));
        emitter.emit(new QueueEvent(EventName.QUEUE_PAUSED, "emails"));
        emitter.emit(new QueueEvent(EventName.QUEUE_RESUMED, "emails")); // nobody listens

        assertEquals(List.of(new EnqueuedEvent("j1", "t", "default")), enqueues);
        assertEquals(List.of(new QueueEvent(EventName.QUEUE_PAUSED, "emails")), pauses);
    }

    @Test
    void legacyOnRejectsNonOutcomeNames() {
        Emitter emitter = new Emitter();
        assertThrows(IllegalArgumentException.class, () -> emitter.on(EventName.JOB_ENQUEUED, event -> {}));
        assertThrows(IllegalArgumentException.class, () -> emitter.on(EventName.WORKER_STARTED, event -> {}));
    }

    @Test
    void legacyOnStillReceivesOutcomes() {
        Emitter emitter = new Emitter();
        List<OutcomeEvent> seen = new CopyOnWriteArrayList<>();
        emitter.on(EventName.SUCCESS, seen::add);
        OutcomeEvent event = new OutcomeEvent(EventName.SUCCESS, "j", "t", null, -1, false);
        emitter.emit(event);
        assertEquals(List.of(event), seen);
    }

    @Test
    void forwardsEveryEventToTheParent() {
        Emitter parent = new Emitter();
        Emitter child = new Emitter(parent);
        List<FlexiQEvent> parentSaw = new CopyOnWriteArrayList<>();
        List<FlexiQEvent> childSaw = new CopyOnWriteArrayList<>();
        parent.onEvent(EventName.WORKER_STARTED, parentSaw::add);
        child.onEvent(EventName.WORKER_STARTED, childSaw::add);

        WorkerEvent event = new WorkerEvent(EventName.WORKER_STARTED, List.of("q1"));
        child.emit(event);

        assertEquals(List.of(event), childSaw);
        assertEquals(List.of(event), parentSaw);
    }

    @Test
    void throwingListenerNeverStarvesTheRest() {
        Emitter emitter = new Emitter();
        List<FlexiQEvent> seen = new CopyOnWriteArrayList<>();
        emitter.onEvent(EventName.PREDICATE_REJECTED, event -> {
            throw new IllegalStateException("boom");
        });
        emitter.onEvent(EventName.PREDICATE_REJECTED, seen::add);

        emitter.emit(new PredicateEvent("t", "nope"));
        assertTrue(seen.contains(new PredicateEvent("t", "nope")));
    }
}
