package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.OutcomeEvent;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.byteveda.flexiq.task.RetryPolicy;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkerTest {

    @SuppressWarnings("unchecked")
    private static Class<Map<String, Object>> mapType() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    @Test
    @Timeout(30)
    void runsTaskToCompletion(@TempDir Path dir) throws Exception {
        Task<Map<String, Object>> add = Task.of("add", mapType());
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("a", 2);
            payload.put("b", 3);
            String id = queue.enqueue(add, payload);

            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(
                            add,
                            (Map<String, Object> p) ->
                                    ((Number) p.get("a")).intValue() + ((Number) p.get("b")).intValue())
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "task should complete");

                Optional<byte[]> result = queue.getResult(id);
                assertTrue(result.isPresent());
                assertEquals("5", new String(result.get(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    @Timeout(30)
    void middlewareFaultDoesNotStarveOthers(@TempDir Path dir) throws Exception {
        Task<Map<String, Object>> noop = Task.of("noop", mapType());
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            CountDownLatch secondSaw = new CountDownLatch(1);
            queue.use(new Middleware() {
                @Override
                public void onCompleted(OutcomeEvent event) {
                    throw new IllegalStateException("boom");
                }
            });
            queue.use(new Middleware() {
                @Override
                public void onCompleted(OutcomeEvent event) {
                    secondSaw.countDown();
                }
            });
            queue.enqueue(noop, new HashMap<>());
            Worker worker = queue.worker().handle(noop, p -> "ok").start();
            try (worker) {
                assertTrue(secondSaw.await(20, TimeUnit.SECONDS), "second middleware must still see the outcome");
            }
        }
    }

    @Test
    @Timeout(30)
    void reportsHowLongTheTaskRan(@TempDir Path dir) throws Exception {
        Task<Map<String, Object>> slow = Task.of("slow", mapType());
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            AtomicLong middlewareElapsed = new AtomicLong(-1);
            queue.use(new Middleware() {
                @Override
                public void after(TaskContext context, Object result) {
                    middlewareElapsed.set(context.elapsedMs());
                }
            });
            queue.enqueue(slow, new HashMap<>());

            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<OutcomeEvent> outcome = new AtomicReference<>();
            Worker worker = queue.worker()
                    .handle(slow, p -> {
                        Thread.sleep(60);
                        return "ok";
                    })
                    .on(EventName.SUCCESS, event -> {
                        outcome.set(event);
                        done.countDown();
                    })
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "task should complete");
            }

            // Both clocks measure a run that slept 60ms; allow for timer jitter.
            Long duration = outcome.get().durationMs();
            assertNotNull(duration, "a job the worker ran must report its duration");
            assertTrue(duration >= 50, "outcome duration was " + duration + "ms");
            assertTrue(middlewareElapsed.get() >= 50, "middleware elapsed was " + middlewareElapsed.get() + "ms");
        }
    }

    @Test
    @Timeout(30)
    void emitsJobFailedBeforeTheRetryOutcome(@TempDir Path dir) throws Exception {
        Task<String> flaky = Task.of("flaky", String.class)
                .maxRetries(3)
                .retryPolicy(RetryPolicy.delays(Duration.ofMillis(10), Duration.ofMillis(10)));
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(flaky, "go");
            List<EventName> order = new CopyOnWriteArrayList<>();
            AtomicInteger attempts = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(flaky, (String payload) -> {
                        if (attempts.incrementAndGet() == 1) {
                            throw new IllegalStateException("first attempt fails");
                        }
                        return "ok";
                    })
                    .onEvent(EventName.JOB_FAILED, event -> order.add(event.name()))
                    .on(EventName.RETRY, event -> order.add(event.name()))
                    .start();
            try (worker) {
                pollUntil(() -> order.contains(EventName.RETRY), "retry outcome never arrived");
                int failedAt = order.indexOf(EventName.JOB_FAILED);
                assertTrue(failedAt >= 0, "job.failed must fire for the failing attempt");
                assertTrue(failedAt < order.indexOf(EventName.RETRY), "job.failed must precede the retry outcome");
            }
        }
    }

    @Test
    @Timeout(30)
    void emitsWorkerLifecycleInOrder(@TempDir Path dir) {
        Task<Map<String, Object>> noop = Task.of("noop", mapType());
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            List<EventName> lifecycle = new CopyOnWriteArrayList<>();
            // Subscribed at the queue: worker events must reach queue-level listeners.
            for (EventName name : List.of(
                    EventName.WORKER_STARTED,
                    EventName.WORKER_ONLINE,
                    EventName.WORKER_STOPPED,
                    EventName.WORKER_OFFLINE)) {
                queue.onEvent(name, event -> lifecycle.add(event.name()));
            }
            Worker worker = queue.worker().handle(noop, p -> "ok").start();
            worker.stop(); // stopped fires here...
            worker.close(); // ...and close() must not repeat it before going offline
            assertEquals(
                    List.of(
                            EventName.WORKER_STARTED,
                            EventName.WORKER_ONLINE,
                            EventName.WORKER_STOPPED,
                            EventName.WORKER_OFFLINE),
                    lifecycle);
        }
    }

    /** Poll {@code condition} until it holds, or fail after 20s. */
    private static void pollUntil(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        fail(message);
    }
}
