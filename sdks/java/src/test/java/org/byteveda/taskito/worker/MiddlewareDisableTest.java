package org.byteveda.taskito.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.middleware.Middleware;
import org.byteveda.taskito.middleware.TaskContext;
import org.byteveda.taskito.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Per-task middleware disables take effect without restarting the worker. */
class MiddlewareDisableTest {

    /** Counts how many times its before/after ran, so a skip is visible. */
    static final class Counting implements Middleware {
        final AtomicInteger before = new AtomicInteger();
        final AtomicInteger after = new AtomicInteger();

        @Override
        public void before(TaskContext context) {
            before.incrementAndGet();
        }

        @Override
        public void after(TaskContext context, Object result) {
            after.incrementAndGet();
        }
    }

    /** Poll the aggregate rather than rendezvous the running worker on a barrier. */
    private static void awaitCompleted(Taskito queue, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (queue.stats().completed < expected && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(expected, queue.stats().completed, "expected " + expected + " completed jobs");
    }

    @Test
    @Timeout(30)
    void disableTakesEffectOnTheNextJobWithoutARestart(@TempDir Path dir) throws Exception {
        Task<String> counted = Task.of("counted", String.class);
        Counting counting = new Counting();

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.use(counting);

            CountDownLatch first = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(counted, (String p) -> p)
                    .on(EventName.SUCCESS, event -> first.countDown())
                    .start();
            try (worker) {
                queue.enqueue(counted, "1");
                assertTrue(first.await(20, TimeUnit.SECONDS), "first job should run");
                assertEquals(1, counting.before.get(), "middleware runs while enabled");

                // Toggle mid-worker: the list is read per invocation, so the next
                // job must skip it without the worker being restarted.
                queue.disableMiddleware("counted", Counting.class.getName());
                assertEquals(List.of(Counting.class.getName()), queue.listDisabledMiddleware("counted"));

                queue.enqueue(counted, "2");
                awaitCompleted(queue, 2);
                assertEquals(1, counting.before.get(), "disabled middleware must not run");

                // Re-enabling restores it, again with no restart.
                queue.enableMiddleware("counted", Counting.class.getName());
                assertEquals(List.of(), queue.listDisabledMiddleware("counted"));
                queue.enqueue(counted, "3");
                awaitCompleted(queue, 3);
                assertEquals(2, counting.before.get(), "re-enabled middleware runs again");
            }
        }
    }

    @Test
    @Timeout(30)
    void beforeAndAfterShareOneResolvedChain(@TempDir Path dir) throws Exception {
        // The chain is resolved once per invocation, so a toggle landing while a
        // job is in flight cannot run after on a middleware whose before did not.
        Task<String> slow = Task.of("slow", String.class);
        Counting counting = new Counting();

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.use(counting);

            CountDownLatch started = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(slow, (String p) -> {
                        started.countDown();
                        try {
                            Thread.sleep(400);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(slow, "1");
                assertTrue(started.await(20, TimeUnit.SECONDS), "handler should start");

                // Disable while the job is mid-flight.
                queue.disableMiddleware("slow", Counting.class.getName());

                awaitCompleted(queue, 1);
            }
        }

        assertEquals(
                counting.before.get(),
                counting.after.get(),
                "before and after must stay balanced across a mid-job toggle");
    }
}
