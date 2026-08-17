package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.task.RetryPolicy;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A task's {@code retryOn} predicate classifies its failures: a rejected
 * exception dead-letters at once, an accepted one still spends the retry budget.
 */
class RetryPredicateTest {

    private static final RetryPolicy FAST = RetryPolicy.delays(Duration.ofMillis(10), Duration.ofMillis(10));

    @Test
    @Timeout(30)
    void rejectedExceptionDeadLettersWithoutSpendingTheBudget(@TempDir Path dir) throws Exception {
        Task<String> permanent = Task.of("permanent", String.class)
                .maxRetries(3)
                .retryPolicy(FAST)
                .retryOn(error -> !(error instanceof IllegalArgumentException));

        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(permanent, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger retries = new AtomicInteger();
            CountDownLatch dead = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(permanent, (String payload) -> {
                        attempts.incrementAndGet();
                        throw new IllegalArgumentException("malformed input");
                    })
                    .on(EventName.RETRY, event -> retries.incrementAndGet())
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                assertTrue(dead.await(25, TimeUnit.SECONDS), "task should dead-letter");

                assertEquals(1, attempts.get(), "a rejected exception must not be retried");
                assertEquals(0, retries.get(), "no RETRY outcome should be emitted");
            }
        }
    }

    @Test
    @Timeout(30)
    void acceptedExceptionStillRetries(@TempDir Path dir) throws Exception {
        Task<String> transientFailure = Task.of("transient", String.class)
                .maxRetries(2)
                .retryPolicy(FAST)
                .retryOn(error -> !(error instanceof IllegalArgumentException));

        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(transientFailure, "go");

            AtomicInteger attempts = new AtomicInteger();
            CountDownLatch dead = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(transientFailure, (String payload) -> {
                        attempts.incrementAndGet();
                        throw new IllegalStateException("connection reset");
                    })
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                assertTrue(dead.await(25, TimeUnit.SECONDS), "task should exhaust its budget");

                assertEquals(3, attempts.get(), "first run plus both retries");
            }
        }
    }
}
