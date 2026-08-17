package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
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
 * A handler that fails twice then succeeds must be retried by the core scheduler
 * — proving the per-task {@link RetryPolicy} is wired through to the native retry
 * engine (RETRY outcomes fire; no Java-side re-enqueue emulation).
 */
class RetryPolicyTest {

    @Test
    @Timeout(30)
    void failingTaskIsRetriedUntilItSucceeds(@TempDir Path dir) throws Exception {
        Task<String> flaky = Task.of("flaky", String.class)
                .maxRetries(3)
                .retryPolicy(RetryPolicy.delays(Duration.ofMillis(10), Duration.ofMillis(10)));

        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            String id = queue.enqueue(flaky, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger retries = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(flaky, (String payload) -> {
                        if (attempts.incrementAndGet() < 3) {
                            throw new IllegalStateException("transient failure");
                        }
                        return 42;
                    })
                    .on(EventName.RETRY, event -> retries.incrementAndGet())
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "task should eventually succeed");

                assertEquals(3, attempts.get(), "should run three times (two failures, one success)");
                assertEquals(2, retries.get(), "core should emit a RETRY outcome per failure");

                Optional<byte[]> result = queue.getResult(id);
                assertTrue(result.isPresent());
                assertEquals("42", new String(result.get(), StandardCharsets.UTF_8));
            }
        }
    }
}
