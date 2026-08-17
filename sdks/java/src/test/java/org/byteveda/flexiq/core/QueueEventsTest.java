package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.PredicateRejectedException;
import org.byteveda.flexiq.events.EnqueuedEvent;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.FlexiQEvent;
import org.byteveda.flexiq.events.PredicateEvent;
import org.byteveda.flexiq.events.QueueEvent;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueueEventsTest {

    private static final Task<Integer> TASK = Task.of("ev.task", Integer.class);

    @Test
    void enqueueEmitsJobEnqueued(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("ev.db").toString()).open()) {
            List<FlexiQEvent> seen = new CopyOnWriteArrayList<>();
            queue.onEvent(EventName.JOB_ENQUEUED, seen::add);

            String jobId = queue.enqueue(TASK, 1);

            assertEquals(List.of(new EnqueuedEvent(jobId, "ev.task", "default")), seen);
        }
    }

    @Test
    void batchEnqueueEmitsOneEventPerJob(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("evb.db").toString()).open()) {
            List<FlexiQEvent> seen = new CopyOnWriteArrayList<>();
            queue.onEvent(EventName.JOB_ENQUEUED, seen::add);

            List<String> jobIds = queue.enqueueMany(TASK, List.of(1, 2, 3));

            assertEquals(3, seen.size());
            for (int i = 0; i < jobIds.size(); i++) {
                assertEquals(new EnqueuedEvent(jobIds.get(i), "ev.task", "default"), seen.get(i));
            }
        }
    }

    @Test
    void predicateRejectionEmitsOnBothEnqueuePaths(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("evp.db").toString()).open()) {
            queue.predicate("ev.task", ctx -> (Integer) ctx.payload() > 0);
            List<FlexiQEvent> seen = new CopyOnWriteArrayList<>();
            queue.onEvent(EventName.PREDICATE_REJECTED, seen::add);

            assertThrows(PredicateRejectedException.class, () -> queue.enqueue(TASK, -1));
            assertThrows(PredicateRejectedException.class, () -> queue.enqueueMany(TASK, List.of(1, -1)));

            assertEquals(2, seen.size());
            assertTrue(seen.stream().allMatch(event -> "ev.task".equals(((PredicateEvent) event).taskName())));
        }
    }

    @Test
    void pauseAndResumeEmitQueueEvents(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("evq.db").toString()).open()) {
            List<FlexiQEvent> seen = new CopyOnWriteArrayList<>();
            queue.onEvent(EventName.QUEUE_PAUSED, seen::add);
            queue.onEvent(EventName.QUEUE_RESUMED, seen::add);

            queue.queue("emails").pause();
            queue.queue("emails").resume();

            assertEquals(
                    List.of(
                            new QueueEvent(EventName.QUEUE_PAUSED, "emails"),
                            new QueueEvent(EventName.QUEUE_RESUMED, "emails")),
                    seen);
        }
    }
}
