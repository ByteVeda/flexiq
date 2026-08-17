package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.EnqueueSkippedException;
import org.byteveda.flexiq.errors.PredicateRejectedException;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.predicates.EnqueueDecision;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnqueueDecisionTest {

    private static final Task<String> TASK = Task.of("ed.echo", String.class);

    @Test
    void skipReturnsEmptyFromTryEnqueueAndThrowsFromEnqueue(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir, "skip")) {
            queue.gate(TASK.name(), context -> EnqueueDecision.skip("not now"));

            assertEquals(Optional.empty(), queue.tryEnqueue(TASK, "hi"));
            assertThrows(EnqueueSkippedException.class, () -> queue.enqueue(TASK, "hi"));
        }
    }

    @Test
    void rejectThrowsFromBothPaths(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir, "reject")) {
            queue.gate(TASK.name(), context -> EnqueueDecision.reject("nope"));

            assertThrows(PredicateRejectedException.class, () -> queue.enqueue(TASK, "hi"));
            assertThrows(PredicateRejectedException.class, () -> queue.tryEnqueue(TASK, "hi"));
        }
    }

    @Test
    void deferDelaysTheScheduledTime(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir, "defer")) {
            queue.gate(TASK.name(), context -> EnqueueDecision.defer(Duration.ofHours(1)));

            Optional<String> id = queue.tryEnqueue(TASK, "later");
            assertTrue(id.isPresent());
            Job job = queue.getJob(id.get()).orElseThrow();
            long delayMs = job.scheduledAt - job.createdAt;
            assertTrue(delayMs >= Duration.ofMinutes(59).toMillis(), "expected ~1h defer, got " + delayMs + "ms");
        }
    }

    @Test
    void zeroDeferOverridesTaskDelay(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir, "deferzero")) {
            queue.gate(TASK.name(), context -> EnqueueDecision.defer(Duration.ZERO));

            Task<String> delayed =
                    TASK.withOptions(EnqueueOptions.builder().delayMs(60_000).build());
            Optional<String> id = queue.tryEnqueue(delayed, "now");
            assertTrue(id.isPresent());
            Job job = queue.getJob(id.get()).orElseThrow();
            // Defer(ZERO) means "enqueue now" — the task's baked-in delay must not survive.
            assertTrue(
                    job.scheduledAt - job.createdAt < 1_000,
                    "expected immediate schedule, got " + (job.scheduledAt - job.createdAt) + "ms");
        }
    }

    @Test
    void allowEnqueuesNormally(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir, "allow")) {
            queue.gate(TASK.name(), context -> EnqueueDecision.allow());

            String id = queue.enqueue(TASK, "hi");
            assertTrue(queue.getJob(id).isPresent());
        }
    }

    @Test
    void booleanPredicateStillRejects(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir, "pred")) {
            queue.predicate(TASK.name(), context -> false);

            assertThrows(PredicateRejectedException.class, () -> queue.enqueue(TASK, "hi"));
        }
    }

    @Test
    void firstNonAllowGateWins(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir, "order")) {
            queue.gate(TASK.name(), context -> EnqueueDecision.skip("first"));
            queue.gate(TASK.name(), context -> EnqueueDecision.reject("second"));

            // Skip is registered first, so the enqueue is skipped (not rejected).
            assertFalse(queue.tryEnqueue(TASK, "hi").isPresent());
        }
    }

    private static FlexiQ open(Path dir, String name) {
        return FlexiQ.builder().url(dir.resolve(name + ".db").toString()).open();
    }
}
