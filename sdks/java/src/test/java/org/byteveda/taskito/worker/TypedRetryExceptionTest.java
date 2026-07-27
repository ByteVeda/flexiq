package org.byteveda.taskito.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.NonRetryableException;
import org.byteveda.taskito.errors.RetryableException;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.task.RetryPolicy;
import org.byteveda.taskito.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A handler signals its retry intent by exception type: {@link NonRetryableException}
 * dead-letters at once, {@link RetryableException} spends the budget — both over the
 * task's {@code retryOn} predicate.
 */
class TypedRetryExceptionTest {

    private static final RetryPolicy FAST = RetryPolicy.delays(Duration.ofMillis(10), Duration.ofMillis(10));

    @Test
    @Timeout(30)
    void nonRetryableExceptionDeadLettersWithoutSpendingTheBudget(@TempDir Path dir) throws Exception {
        Task<String> charge = Task.of("charge", String.class).maxRetries(3).retryPolicy(FAST);

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(charge, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger retries = new AtomicInteger();
            CountDownLatch dead = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(charge, (String payload) -> {
                        attempts.incrementAndGet();
                        throw new NonRetryableException("card declined");
                    })
                    .on(EventName.RETRY, event -> retries.incrementAndGet())
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                assertTrue(dead.await(25, TimeUnit.SECONDS), "task should dead-letter");

                assertEquals(1, attempts.get(), "a permanent failure must not be retried");
                assertEquals(0, retries.get(), "no RETRY outcome should be emitted");
            }
        }
    }

    @Test
    @Timeout(30)
    void retryableExceptionOverridesARejectingPredicate(@TempDir Path dir) throws Exception {
        Task<String> sync = Task.of("sync", String.class)
                .maxRetries(2)
                .retryPolicy(FAST)
                .retryOn(error -> false); // would dead-letter everything

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(sync, "go");

            AtomicInteger attempts = new AtomicInteger();
            CountDownLatch dead = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(sync, (String payload) -> {
                        attempts.incrementAndGet();
                        throw new RetryableException("upstream 503");
                    })
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                assertTrue(dead.await(25, TimeUnit.SECONDS), "task should exhaust its budget");

                assertEquals(3, attempts.get(), "first run plus both retries");
            }
        }
    }

    @Test
    @Timeout(30)
    void aWrappedNonRetryableSignalStillDeadLetters(@TempDir Path dir) throws Exception {
        Task<String> parse = Task.of("parse", String.class)
                .maxRetries(3)
                .retryPolicy(FAST)
                .retryOn(error -> true); // would retry everything

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(parse, "go");

            AtomicInteger attempts = new AtomicInteger();
            CountDownLatch dead = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(parse, (String payload) -> {
                        attempts.incrementAndGet();
                        throw new IllegalStateException("row 3", new NonRetryableException("malformed input"));
                    })
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                assertTrue(dead.await(25, TimeUnit.SECONDS), "task should dead-letter");

                assertEquals(1, attempts.get(), "a wrapped permanent failure must not be retried");
            }
        }
    }
}
