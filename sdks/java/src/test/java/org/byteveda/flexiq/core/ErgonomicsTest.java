package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.locks.Lock;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.byteveda.flexiq.workflows.FanMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class ErgonomicsTest {

    @Test
    @Timeout(30)
    void awaitJobReturnsTerminalState(@TempDir Path dir) throws Exception {
        Task<Integer> echo = Task.of("erg.echo", Integer.class).retries(0).timeout(Duration.ofSeconds(10));
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("erg.db").toString()).open()) {
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
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("lock.db").toString()).open()) {
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
