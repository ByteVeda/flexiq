package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Dead-letter entries can be listed and purged scoped to a single task. */
class DeadLetterByTaskTest {

    @Test
    @Timeout(30)
    void listAndPurgeByTask(@TempDir Path dir) throws Exception {
        // maxRetries(0) → a thrown handler dead-letters immediately.
        Task<String> alpha = Task.of("alpha", String.class).maxRetries(0);
        Task<String> beta = Task.of("beta", String.class).maxRetries(0);

        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(alpha, "1");
            queue.enqueue(alpha, "2");
            queue.enqueue(beta, "3");

            CountDownLatch dead = new CountDownLatch(3);
            Worker worker = queue.worker()
                    .handle(alpha, (String p) -> {
                        throw new IllegalStateException("boom");
                    })
                    .handle(beta, (String p) -> {
                        throw new IllegalStateException("boom");
                    })
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                assertTrue(dead.await(20, TimeUnit.SECONDS), "all three should dead-letter");
            }

            assertEquals(2, queue.listDeadByTask("alpha", 10, 0).size());
            assertEquals(1, queue.listDeadByTask("beta", 10, 0).size());
            // Pagination applies within the task's own entries.
            assertEquals(1, queue.listDeadByTask("alpha", 1, 1).size());

            assertEquals(2, queue.purgeDeadByTask("alpha"));
            assertEquals(0, queue.listDeadByTask("alpha", 10, 0).size());
            assertEquals(1, queue.listDead(10, 0).size(), "beta entry survives");
        }
    }
}
