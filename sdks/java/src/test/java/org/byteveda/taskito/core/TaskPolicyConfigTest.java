package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Per-task throttling and concurrency caps reach the scheduler. */
class TaskPolicyConfigTest {

    /** A cap of one means jobs of this task never overlap, however many the worker runs. */
    @Test
    @Timeout(30)
    void maxConcurrentSerializesOneTask(@TempDir Path dir) throws Exception {
        Task<String> solo = Task.of("solo", String.class).maxConcurrent(1);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(4);

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            for (int i = 0; i < 4; i++) {
                queue.enqueue(solo, String.valueOf(i));
            }

            Worker worker = queue.worker()
                    .concurrency(4)
                    .handle(solo, (String p) -> {
                        peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                        try {
                            Thread.sleep(150);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        running.decrementAndGet();
                        return null;
                    })
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "all four jobs should finish");
            }
        }

        assertEquals(1, peak.get(), "maxConcurrent=1 must never let two jobs of the task overlap");
    }

    /** The in-process bulkhead holds a task to its share of one worker's slots. */
    @Test
    @Timeout(30)
    void maxInFlightPerTaskBoundsOneTask(@TempDir Path dir) throws Exception {
        // Same visible effect as maxConcurrent=1 here, but enforced in-process
        // from the worker's in-flight map rather than a running-count query.
        Task<String> solo = Task.of("solo", String.class).maxInFlightPerTask(1);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(4);

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            for (int i = 0; i < 4; i++) {
                queue.enqueue(solo, String.valueOf(i));
            }

            Worker worker = queue.worker()
                    .concurrency(4)
                    .batchSize(4)
                    .handle(solo, (String p) -> {
                        peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                        try {
                            Thread.sleep(150);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        running.decrementAndGet();
                        return null;
                    })
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "all four jobs should finish");
            }
        }

        assertEquals(1, peak.get(), "maxInFlightPerTask=1 must never let two jobs of the task overlap");
    }

    /** Throttling drags a burst out past the window it would otherwise finish in. */
    @Test
    @Timeout(30)
    void rateLimitThrottlesDispatch(@TempDir Path dir) throws Exception {
        // The bucket starts full, so the spec's count is also its burst: "60/m"
        // would let 60 jobs through at once and prove nothing. "1/s" holds one
        // token, so of three jobs the first runs at once and the rest wait for a
        // refill — roughly a second each.
        Task<String> limited = Task.of("limited", String.class).rateLimit("1/s");
        CountDownLatch done = new CountDownLatch(3);

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            for (int i = 0; i < 3; i++) {
                queue.enqueue(limited, String.valueOf(i));
            }

            long start = System.nanoTime();
            Worker worker = queue.worker()
                    .handle(limited, (String p) -> null)
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "all three jobs should finish");
            }
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            assertTrue(
                    elapsed.toMillis() >= 1200,
                    "a throttled burst must not finish instantly, took " + elapsed.toMillis() + "ms");
        }
    }

    /** The budget caps retries across jobs, so a storm dead-letters rather than retrying forever. */
    @Test
    @Timeout(30)
    void retryBudgetDeadLettersOnceSpent(@TempDir Path dir) throws Exception {
        // maxRetries(5) would allow plenty of retries per job; one budget token
        // across all of them means the rest dead-letter instead.
        Task<String> flaky = Task.of("flaky", String.class).retryBudget("1/m").maxRetries(5);
        CountDownLatch dead = new CountDownLatch(1);

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            for (int i = 0; i < 3; i++) {
                queue.enqueue(flaky, String.valueOf(i));
            }

            Worker worker = queue.worker()
                    .concurrency(4)
                    .handle(flaky, (String p) -> {
                        throw new IllegalStateException("dependency down");
                    })
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                assertTrue(dead.await(20, TimeUnit.SECONDS), "an over-budget job should dead-letter");
            }

            long budgetKilled = queue.listDead(10, 0).stream()
                    .filter(entry -> "retry_budget_exhausted".equals(entry.metadata))
                    .count();
            assertTrue(budgetKilled > 0, "a job should be dead-lettered by the budget, not by retry exhaustion");
        }
    }

    /** A typo must fail the start, not silently run the task unthrottled. */
    @Test
    @Timeout(30)
    void malformedRateLimitFailsTheStart(@TempDir Path dir) {
        Task<String> bad = Task.of("bad", String.class).rateLimit("100/mm");

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            RuntimeException error = assertThrows(
                    RuntimeException.class,
                    () -> queue.worker().handle(bad, (String p) -> null).start());
            assertTrue(
                    error.getMessage().contains("rateLimit"),
                    "the error should name the offending option, got: " + error.getMessage());
        }
    }
}
