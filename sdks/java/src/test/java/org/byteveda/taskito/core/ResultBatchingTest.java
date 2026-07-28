package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Results finalized as a batch still surface exactly one outcome per job. */
class ResultBatchingTest {

    @Test
    @Timeout(30)
    void mixedBatchReportsEveryJobOnce(@TempDir Path dir) throws Exception {
        // The drain loop finalizes everything already queued in one transaction.
        // Successes batch into a single write while failures stay on the
        // per-result path, so a mixed burst exercises both halves: every job must
        // surface exactly one outcome, none lost and none double-counted.
        int each = 6;
        Task<String> ok = Task.of("ok", String.class);
        Task<String> boom = Task.of("boom", String.class).maxRetries(0);

        ConcurrentLinkedQueue<String> succeeded = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> dead = new ConcurrentLinkedQueue<>();
        CountDownLatch finished = new CountDownLatch(each * 2);

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            for (int i = 0; i < each; i++) {
                queue.enqueue(ok, String.valueOf(i));
                queue.enqueue(boom, String.valueOf(i));
            }

            Worker worker = queue.worker()
                    .concurrency(8)
                    .batchSize(each * 2)
                    .handle(ok, (String p) -> p)
                    .handle(boom, (String p) -> {
                        throw new IllegalStateException("expected");
                    })
                    .on(EventName.SUCCESS, event -> {
                        succeeded.add(event.jobId);
                        finished.countDown();
                    })
                    .on(EventName.DEAD, event -> {
                        dead.add(event.jobId);
                        finished.countDown();
                    })
                    .start();
            try (worker) {
                assertTrue(finished.await(20, TimeUnit.SECONDS), "every job should report an outcome");
            }
        }

        List<String> succeededIds = List.copyOf(succeeded);
        List<String> deadIds = List.copyOf(dead);
        assertEquals(each, succeededIds.size(), "one success outcome per successful job");
        assertEquals(each, deadIds.size(), "one dead outcome per failing job");
        assertEquals(each, succeededIds.stream().collect(Collectors.toSet()).size(), "no job may be reported twice");
        Set<String> overlap = succeededIds.stream().filter(deadIds::contains).collect(Collectors.toSet());
        assertTrue(overlap.isEmpty(), "a job cannot both succeed and dead-letter: " + overlap);
    }
}
