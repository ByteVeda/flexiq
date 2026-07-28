package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.locks.Lock;
import org.byteveda.taskito.model.Job;
import org.byteveda.taskito.model.JobStatus;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.byteveda.taskito.workflows.FanMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class ErgonomicsTest {

    @Test
    @Timeout(30)
    void awaitJobReturnsTerminalState(@TempDir Path dir) throws Exception {
        Task<Integer> echo = Task.of("erg.echo", Integer.class).retries(0).timeout(Duration.ofSeconds(10));
        try (Taskito queue =
                Taskito.builder().sqlite(dir.resolve("erg.db").toString()).open()) {
            String id = queue.enqueue(echo, 42);
            Worker worker = queue.worker().handle(echo, p -> p).start();
            try (worker) {
                Job job = queue.awaitJob(id, Duration.ofSeconds(20)).orElseThrow();
                assertEquals(JobStatus.COMPLETE, job.status);
                assertEquals(42, queue.getResult(id, Integer.class).orElseThrow());
            }
        }
    }

    @Test
    void fanModeWireStrings() {
        assertEquals("each", FanMode.EACH.wire());
        assertEquals("all", FanMode.ALL.wire());
    }

    @Test
    @Timeout(30)
    void lockSugar(@TempDir Path dir) {
        try (Taskito queue =
                Taskito.builder().sqlite(dir.resolve("lock.db").toString()).open()) {
            try (Lock lock = queue.lock("erg-lock")) { // default-TTL overload
                assertTrue(lock.tryAcquire(Duration.ofSeconds(1)));
                assertTrue(queue.getLockInfo("erg-lock").isPresent());
            }
            // released by close(); a fresh holder can re-acquire immediately
            try (Lock again = queue.lock("erg-lock", 5_000)) {
                assertTrue(again.acquire());
            }
            assertFalse(queue.getLockInfo("erg-lock").isPresent());
        }
    }
}
