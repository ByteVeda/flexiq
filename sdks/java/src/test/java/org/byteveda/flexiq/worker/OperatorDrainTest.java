package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.WorkerInfo;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** An operator's drain request, read back on the heartbeat, closes the one worker it names. */
class OperatorDrainTest {

    private static final Task<Integer> TASK = Task.of("drain.task", Integer.class);

    @Test
    @Timeout(60)
    void aDrainedWorkerFinishesItsJobAndUnregisters(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("d.db").toString()).open()) {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch finished = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(TASK, payload -> {
                        started.countDown();
                        release.await(30, TimeUnit.SECONDS);
                        finished.countDown();
                        return payload;
                    })
                    .start();
            queue.enqueue(TASK, 1, EnqueueOptions.builder().build());
            assertTrue(started.await(20, TimeUnit.SECONDS), "the handler never ran");

            String workerId = only(queue.listWorkers()).workerId;
            assertTrue(queue.drainWorker(workerId));
            assertEquals("draining", only(queue.listWorkers()).status);

            CompletableFuture<Void> closed = CompletableFuture.runAsync(() -> {
                try {
                    worker.awaitShutdown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            release.countDown();
            // The heartbeat reads the request within one interval; the close then
            // waits out the running handler rather than abandoning it.
            closed.get(30, TimeUnit.SECONDS);
            assertEquals(0, finished.getCount(), "the drain abandoned a running job");
            assertTrue(waitFor(() -> queue.listWorkers().isEmpty()), "a drained worker left its row behind");
        }
    }

    @Test
    @Timeout(60)
    void aDrainReachesOnlyThisNamespacesWorkers(@TempDir Path dir) throws Exception {
        String url = dir.resolve("ns.db").toString();
        try (FlexiQ mine = FlexiQ.builder().url(url).namespace("mine").open();
                FlexiQ theirs = FlexiQ.builder().url(url).namespace("theirs").open()) {
            Worker worker = theirs.worker().handle(TASK, payload -> payload).start();
            try {
                String workerId = only(theirs.listWorkers()).workerId;
                assertFalse(mine.drainWorker(workerId));
                assertFalse(mine.drainWorker("no-such-worker"));
                assertEquals("active", only(theirs.listWorkers()).status);
            } finally {
                worker.close();
            }
        }
    }

    private static WorkerInfo only(List<WorkerInfo> workers) {
        assertEquals(1, workers.size(), "workers: " + workers);
        return workers.get(0);
    }

    private static boolean waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }
}
