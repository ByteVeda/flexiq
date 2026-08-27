package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.QueueFullException;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** S26 — opt-in {@code maxPending} admission cap. No worker runs, so jobs stay pending. */
class AdmissionTest {

    private static FlexiQ sqlite(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open();
    }

    @Test
    @Timeout(30)
    void countsPendingPerQueue(@TempDir Path dir) {
        Task<String> noop = Task.of("noop", String.class);
        try (FlexiQ queue = sqlite(dir)) {
            assertEquals(0, queue.countPendingByQueue("default"));
            queue.enqueue(noop, "a");
            queue.enqueue(noop, "b");
            assertEquals(2, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(30)
    void uncappedQueueNeverRejects(@TempDir Path dir) {
        Task<String> noop = Task.of("noop", String.class);
        try (FlexiQ queue = sqlite(dir)) {
            for (int i = 0; i < 25; i++) {
                queue.enqueue(noop, String.valueOf(i));
            }
            assertEquals(25, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(30)
    void rejectsAtCap(@TempDir Path dir) {
        Task<String> noop = Task.of("noop", String.class);
        try (FlexiQ queue = sqlite(dir)) {
            queue.maxPending("default", 2);
            queue.enqueue(noop, "a");
            queue.enqueue(noop, "b");
            assertThrows(QueueFullException.class, () -> queue.enqueue(noop, "c"));
            // Rejected enqueue inserted nothing.
            assertEquals(2, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(30)
    void enqueueManyAccountsForBatchSize(@TempDir Path dir) {
        Task<String> noop = Task.of("noop", String.class);
        try (FlexiQ queue = sqlite(dir)) {
            queue.maxPending("default", 3);
            // Empty queue, but a batch bigger than the cap is rejected as a whole.
            assertThrows(
                    QueueFullException.class, () -> queue.enqueueMany(noop, java.util.List.of("a", "b", "c", "d")));
            assertEquals(0, queue.countPendingByQueue("default"));
            // A batch that exactly fits is admitted.
            queue.enqueueMany(noop, java.util.List.of("a", "b", "c"));
            assertEquals(3, queue.countPendingByQueue("default"));
            // Now full: one more is rejected.
            assertThrows(QueueFullException.class, () -> queue.enqueue(noop, "x"));
        }
    }

    @Test
    void rejectsNegativeCap(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir)) {
            assertThrows(IllegalArgumentException.class, () -> queue.maxPending("default", -1));
        }
    }

    /**
     * The cap is admission control on pending rows and a coalescing enqueue adds none, so a
     * full queue still takes a slide onto the open window.
     */
    @Test
    @Timeout(30)
    void fullQueueStillTakesADebouncedSlide(@TempDir Path dir) {
        Task<String> noop = Task.of("noop", String.class);
        Task<String> report =
                Task.of("report", String.class).debounce(Duration.ofMinutes(5), "report:u1", Duration.ofMinutes(30));
        try (FlexiQ queue = sqlite(dir)) {
            queue.maxPending("default", 2);
            String opened = queue.enqueue(report, "u1");
            queue.enqueue(noop, "a");
            assertEquals(2, queue.countPendingByQueue("default"));

            assertEquals(opened, queue.enqueue(report, "u1"));
            assertEquals(2, queue.countPendingByQueue("default"));
        }
    }

    /** With no window open there is a row to insert, so the same full queue refuses it. */
    @Test
    @Timeout(30)
    void fullQueueStillRefusesToOpenADebounceWindow(@TempDir Path dir) {
        Task<String> noop = Task.of("noop", String.class);
        Task<String> report =
                Task.of("report", String.class).debounce(Duration.ofMinutes(5), "report:u1", Duration.ofMinutes(30));
        try (FlexiQ queue = sqlite(dir)) {
            queue.maxPending("default", 2);
            queue.enqueue(noop, "a");
            queue.enqueue(noop, "b");

            QueueFullException full = assertThrows(QueueFullException.class, () -> queue.enqueue(report, "u1"));
            assertEquals("default", full.queue());
            assertEquals(2, full.pending());
            assertEquals(2, full.cap());
            assertEquals(2, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(30)
    void capIsPerQueue(@TempDir Path dir) {
        Task<String> noop = Task.of("noop", String.class);
        try (FlexiQ queue = sqlite(dir)) {
            queue.maxPending("tight", 1);
            EnqueueOptions tight = EnqueueOptions.builder().queue("tight").build();
            queue.enqueue(noop, "a", tight);
            assertThrows(QueueFullException.class, () -> queue.enqueue(noop, "b", tight));
            // A different, uncapped queue is unaffected.
            EnqueueOptions wide = EnqueueOptions.builder().queue("wide").build();
            for (int i = 0; i < 5; i++) {
                queue.enqueue(noop, String.valueOf(i), wide);
            }
        }
    }
}
