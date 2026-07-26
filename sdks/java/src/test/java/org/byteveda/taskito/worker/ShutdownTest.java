package org.byteveda.taskito.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.resources.ResourceScope;
import org.byteveda.taskito.resources.Resources;
import org.byteveda.taskito.task.EnqueueOptions;
import org.byteveda.taskito.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class ShutdownTest {

    private static final Task<Integer> TASK = Task.of("shutdown.task", Integer.class);

    @Test
    @Timeout(30)
    void shutdownClosesEveryWorkerStartedFromTheClient(@TempDir Path dir) throws Exception {
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("sd.db").toString()).open()) {
            queue.resource("res", ResourceScope.WORKER, ctx -> new Object(), value -> disposed.incrementAndGet());

            CountDownLatch ran = new CountDownLatch(2);
            Worker first = startWorker(queue, "a", ran);
            Worker second = startWorker(queue, "b", ran);
            enqueueTo(queue, "a");
            enqueueTo(queue, "b");
            assertTrue(ran.await(20, TimeUnit.SECONDS), "both handlers did not run");

            queue.shutdown();
            // Each worker holds its own WORKER-scoped instance, so both were disposed.
            assertEquals(2, disposed.get());

            // Closing an already-shut-down worker does nothing the second time.
            first.close();
            second.close();
            assertEquals(2, disposed.get());
        }
    }

    @Test
    void shutdownWithoutWorkersIsANoOp(@TempDir Path dir) {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("sn.db").toString()).open()) {
            queue.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void shutdownForgetsWorkersClosedDirectly(@TempDir Path dir) throws Exception {
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("sf.db").toString()).open()) {
            queue.resource("res", ResourceScope.WORKER, ctx -> new Object(), value -> disposed.incrementAndGet());

            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = startWorker(queue, "a", ran);
            enqueueTo(queue, "a");
            assertTrue(ran.await(20, TimeUnit.SECONDS), "handler did not run");

            worker.close();
            assertEquals(1, disposed.get());
            queue.shutdown(); // the worker already left the live set
            assertEquals(1, disposed.get());
        }
    }

    private static Worker startWorker(Taskito queue, String lane, CountDownLatch ran) {
        return queue.worker()
                .queues(lane)
                .handle(TASK, payload -> {
                    Resources.use("res");
                    ran.countDown();
                    return payload;
                })
                .start();
    }

    private static void enqueueTo(Taskito queue, String lane) {
        queue.enqueue(TASK, 1, EnqueueOptions.builder().queue(lane).build());
    }
}
