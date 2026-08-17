package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.batch.Batcher;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class BatcherTest {

    private static final Task<Integer> TASK = Task.of("b.task", Integer.class);

    @Test
    void flushesWhenBatchFull(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("b.db").toString()).open();
                Batcher<Integer> batcher = Batcher.of(queue, TASK, 3, Duration.ofSeconds(60))) {
            assertTrue(batcher.add(1).isEmpty());
            assertTrue(batcher.add(2).isEmpty());
            List<String> ids = batcher.add(3);
            assertEquals(3, ids.size());
            assertEquals(3, queue.stats().pending);
        }
    }

    @Test
    void flushesRemainderOnClose(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("b.db").toString()).open()) {
            try (Batcher<Integer> batcher = Batcher.of(queue, TASK, 100, Duration.ofSeconds(60))) {
                batcher.add(1);
                batcher.add(2);
            }
            assertEquals(2, queue.stats().pending);
        }
    }

    @Test
    void addAfterCloseThrows(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("b.db").toString()).open()) {
            Batcher<Integer> batcher = Batcher.of(queue, TASK, 100, Duration.ofSeconds(60));
            batcher.close();
            assertThrows(IllegalStateException.class, () -> batcher.add(1));
        }
    }

    @Test
    @Timeout(30)
    void flushesAfterDelay(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("b.db").toString()).open();
                Batcher<Integer> batcher = Batcher.of(queue, TASK, 100, Duration.ofMillis(200))) {
            batcher.add(1);
            batcher.add(2);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (queue.stats().pending < 2 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(2, queue.stats().pending);
        }
    }
}
