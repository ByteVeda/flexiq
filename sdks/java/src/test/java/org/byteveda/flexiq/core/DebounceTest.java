package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * S655 — debounce: a burst of enqueues sharing a key collapses onto one pending job whose
 * deadline slides forward, capped by the max wait. No worker runs in the collapse tests, so
 * every job stays pending and each enqueue lands on the open window.
 */
class DebounceTest {

    /** Payload with a field the key template addresses. */
    record Report(String userId, int revision) {}

    private static FlexiQ sqlite(Path dir, String db) {
        return FlexiQ.builder().url(dir.resolve(db).toString()).open();
    }

    private static Task<Report> debounced(String name) {
        return Task.of(name, Report.class).debounce(Duration.ofMinutes(5), "report:{userId}", Duration.ofMinutes(30));
    }

    // ── Collapse ─────────────────────────────────────────────────────

    @Test
    @Timeout(30)
    void burstCollapsesOntoOneJob(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "collapse.db")) {
            Task<Report> task = debounced("debounce.collapse");
            String first = queue.enqueue(task, new Report("u1", 1));
            String second = queue.enqueue(task, new Report("u1", 2));
            String third = queue.enqueue(task, new Report("u1", 3));

            assertEquals(first, second, "a repeat enqueue lands on the open window");
            assertEquals(first, third);
            assertEquals(1, queue.countPendingByQueue("default"), "the burst is one job, not three");
        }
    }

    @Test
    @Timeout(30)
    void repeatEnqueueSlidesTheDeadline(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = sqlite(dir, "slide.db")) {
            // A short window keeps the test fast; the max wait is far enough out not to cap.
            Task<Report> task = Task.of("debounce.slide", Report.class)
                    .debounce(Duration.ofSeconds(5), "report:{userId}", Duration.ofMinutes(10));

            String id = queue.enqueue(task, new Report("u1", 1));
            long firstDeadline = job(queue, id).scheduledAt;
            Thread.sleep(50);
            queue.enqueue(task, new Report("u1", 2));

            assertTrue(
                    job(queue, id).scheduledAt > firstDeadline,
                    "the second enqueue must push the deadline forward, not leave it alone");
        }
    }

    @Test
    @Timeout(30)
    void maxWaitCapsTheSlide(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = sqlite(dir, "maxwait.db")) {
            // maxWait is only 100ms past the window, so the second enqueue's slide is clamped
            // rather than pushing the deadline another full window out.
            Task<Report> task = Task.of("debounce.maxwait", Report.class)
                    .debounce(Duration.ofSeconds(5), "report:{userId}", Duration.ofMillis(5_100));

            String id = queue.enqueue(task, new Report("u1", 1));
            Job opened = job(queue, id);
            Thread.sleep(300);
            queue.enqueue(task, new Report("u1", 2));

            Job slid = job(queue, id);
            assertTrue(
                    slid.scheduledAt <= opened.createdAt + 5_100,
                    "max wait caps the deadline at createdAt + maxWait, got " + (slid.scheduledAt - opened.createdAt)
                            + "ms past open");
            assertTrue(slid.scheduledAt > opened.createdAt, "the job still runs in the future");
        }
    }

    @Test
    @Timeout(30)
    void distinctKeysDebounceIndependently(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "keys.db")) {
            Task<Report> task = debounced("debounce.keys");
            String u1 = queue.enqueue(task, new Report("u1", 1));
            String u2 = queue.enqueue(task, new Report("u2", 1));
            String u1Again = queue.enqueue(task, new Report("u1", 2));

            assertNotEquals(u1, u2, "a different key opens its own window");
            assertEquals(u1, u1Again, "the first key's window is still the one that collapses");
            assertEquals(2, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(30)
    void aLiteralKeyIsOneWindowForTheTask(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "literal.db")) {
            Task<Report> task = Task.of("debounce.literal", Report.class)
                    .debounce(Duration.ofMinutes(5), "nightly", Duration.ofMinutes(30));
            String first = queue.enqueue(task, new Report("u1", 1));
            String second = queue.enqueue(task, new Report("u2", 1));

            assertEquals(first, second, "a template with no placeholder is a deliberate global window");
        }
    }

    @Test
    @Timeout(30)
    void perEnqueueOptionsDebounceWithoutATaskDefault(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "perenqueue.db")) {
            // A producer that registers nothing still gets a window off the options object.
            Task<Report> plain = Task.of("debounce.perenqueue", Report.class);
            EnqueueOptions options = EnqueueOptions.builder()
                    .debounce(Duration.ofMinutes(5))
                    .debounceKey("report:{userId}")
                    .debounceMaxWait(Duration.ofMinutes(30))
                    .build();

            String first = queue.enqueue(plain, new Report("u1", 1), options);
            String second = queue.enqueue(plain, new Report("u1", 2), options);
            assertEquals(first, second);
            assertEquals(1, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(30)
    void replacePayloadRunsTheNewestArguments(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = sqlite(dir, "replace.db")) {
            Task<Report> task = Task.of("debounce.replace", Report.class)
                    // Short window so the collapsed job dispatches inside the test.
                    .debounce(Duration.ofMillis(200), "report:{userId}", Duration.ofSeconds(5), true);
            queue.enqueue(task, new Report("u1", 1));
            queue.enqueue(task, new Report("u1", 2));

            AtomicReference<Report> ran = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(task, report -> {
                        ran.set(report);
                        done.countDown();
                        return null;
                    })
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "the collapsed job should run once the window closes");
            }
            assertEquals(2, ran.get().revision(), "replacePayload runs the newest enqueue's payload");
        }
    }

    // ── Validation ───────────────────────────────────────────────────

    @Test
    void windowWithoutMaxWaitIsRejectedAtBuildTime() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> EnqueueOptions.builder()
                .debounce(Duration.ofMinutes(5))
                .debounceKey("report:{userId}")
                .build());
        assertTrue(e.getMessage().contains("debounceMaxWait"), e.getMessage());
    }

    @Test
    void windowWithoutKeyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> EnqueueOptions.builder()
                .debounce(Duration.ofMinutes(5))
                .debounceMaxWait(Duration.ofMinutes(30))
                .build());
    }

    @Test
    void maxWaitShorterThanTheWindowIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> EnqueueOptions.builder()
                .debounce(Duration.ofMinutes(5))
                .debounceKey("k")
                .debounceMaxWait(Duration.ofMinutes(1))
                .build());
    }

    @Test
    void debounceFieldsWithoutAWindowAreRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> EnqueueOptions.builder().debounceKey("report:{userId}").build());
        assertThrows(IllegalArgumentException.class, () -> EnqueueOptions.builder()
                .debounceMaxWait(Duration.ofMinutes(30))
                .build());
        assertThrows(
                IllegalArgumentException.class,
                () -> EnqueueOptions.builder().debounceReplacePayload(true).build());
    }

    @Test
    void nonPositiveWindowIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> EnqueueOptions.builder()
                .debounce(Duration.ZERO)
                .debounceKey("k")
                .debounceMaxWait(Duration.ofMinutes(1))
                .build());
    }

    // ── Key resolution ───────────────────────────────────────────────

    @Test
    @Timeout(30)
    void anUnresolvablePlaceholderThrowsRatherThanSharingOneWindow(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "unresolvable.db")) {
            Task<Map<String, Object>> task = Task.<Map<String, Object>>of(
                            "debounce.unresolvable", new com.fasterxml.jackson.core.type.TypeReference<>() {})
                    .debounce(Duration.ofMinutes(5), "report:{userId}", Duration.ofMinutes(30));
            IllegalArgumentException e =
                    assertThrows(IllegalArgumentException.class, () -> queue.enqueue(task, Map.of("other", "x")));
            assertTrue(e.getMessage().contains("userId"), e.getMessage());
            assertEquals(0, queue.countPendingByQueue("default"), "a rejected enqueue inserts nothing");
        }
    }

    @Test
    @Timeout(30)
    void anEmptyPropertyCannotKeyAWindow(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "emptykey.db")) {
            // "report:" would be a window every empty-userId payload shares — the same
            // silent collapse a missing property is rejected for.
            Task<Report> task = debounced("debounce.emptykey");
            IllegalArgumentException e =
                    assertThrows(IllegalArgumentException.class, () -> queue.enqueue(task, new Report("", 1)));
            assertTrue(e.getMessage().contains("empty"), e.getMessage());
            assertEquals(0, queue.countPendingByQueue("default"), "a rejected enqueue inserts nothing");
        }
    }

    @Test
    @Timeout(30)
    void aScalarPayloadCannotFillAPlaceholder(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "scalar.db")) {
            Task<String> task = Task.of("debounce.scalar", String.class)
                    .debounce(Duration.ofMinutes(5), "report:{userId}", Duration.ofMinutes(30));
            assertThrows(IllegalArgumentException.class, () -> queue.enqueue(task, "u1"));
        }
    }

    @Test
    @Timeout(30)
    void aNestedPlaceholderWalksIntoTheObject(@TempDir Path dir) {
        record Owner(String id) {}
        record Job2(Owner owner, int revision) {}
        try (FlexiQ queue = sqlite(dir, "nested.db")) {
            Task<Job2> task = Task.of("debounce.nested", Job2.class)
                    .debounce(Duration.ofMinutes(5), "report:{owner.id}", Duration.ofMinutes(30));
            String first = queue.enqueue(task, new Job2(new Owner("u1"), 1));
            String second = queue.enqueue(task, new Job2(new Owner("u1"), 2));
            String other = queue.enqueue(task, new Job2(new Owner("u2"), 1));

            assertEquals(first, second);
            assertNotEquals(first, other);
        }
    }

    // ── Incompatible combinations ────────────────────────────────────

    @Test
    @Timeout(30)
    void debounceAndIdempotencyCannotBeCombined(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "idem.db")) {
            Task<Report> task = debounced("debounce.idem").idempotent(true);
            assertThrows(IllegalArgumentException.class, () -> queue.enqueue(task, new Report("u1", 1)));
        }
    }

    @Test
    @Timeout(30)
    void debounceAndAnExplicitDelayCannotBeCombined(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "delay.db")) {
            Task<Report> task = debounced("debounce.delay");
            EnqueueOptions options =
                    task.options().toBuilder().delay(Duration.ofMinutes(1)).build();
            assertThrows(IllegalArgumentException.class, () -> queue.enqueue(task, new Report("u1", 1), options));
        }
    }

    @Test
    @Timeout(30)
    void batchEnqueueCannotDebounce(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "batch.db")) {
            Task<Report> task = debounced("debounce.batch");
            assertThrows(
                    IllegalArgumentException.class,
                    () -> queue.enqueueMany(task, List.of(new Report("u1", 1), new Report("u2", 1))));
        }
    }

    @Test
    @Timeout(30)
    void aDebouncedTaskCannotBackASubscription(@TempDir Path dir) {
        try (FlexiQ queue = sqlite(dir, "subscribe.db")) {
            Task<Report> task = debounced("debounce.subscribe");
            assertThrows(IllegalArgumentException.class, () -> queue.subscribe("reports", task));
        }
    }

    private static Job job(FlexiQ queue, String id) {
        return queue.getJob(id).orElseThrow(() -> new AssertionError("job " + id + " is gone"));
    }
}
