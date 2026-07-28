package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.model.DispatchOrder;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * S25 — opt-in LIFO dispatch order. Jobs are enqueued before the worker starts,
 * so they pile up pending; a concurrency-1 worker then dispatches them one at a
 * time and we record the order. UUIDv7 ids make same-millisecond ties
 * deterministic, so LIFO is exactly newest-first.
 */
class DispatchOrderTest {

    @Test
    @SuppressWarnings("deprecation")
    void validatesOrder(@TempDir Path dir) {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("v.db").toString()).open()) {
            assertThrows(IllegalArgumentException.class, () -> queue.dispatchOrder("default", "sideways"));
        }
    }

    private static List<Integer> runAndRecord(Taskito queue, int total) throws Exception {
        List<Integer> order = new CopyOnWriteArrayList<>();
        Task<Integer> rec = Task.of("rec", Integer.class);
        for (int i = 0; i < total; i++) {
            queue.enqueue(rec, i);
        }
        Worker worker = queue.worker()
                .concurrency(1)
                .batchSize(1)
                .handle(rec, order::add)
                .start();
        try (worker) {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline && order.size() < total) {
                Thread.sleep(50);
            }
        }
        return order;
    }

    @Test
    @Timeout(40)
    void lifoDispatchesNewestFirst(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("lifo.db").toString()).open()) {
            queue.dispatchOrder("default", DispatchOrder.LIFO);
            List<Integer> order = runAndRecord(queue, 6);
            assertEquals(List.of(5, 4, 3, 2, 1, 0), order, "LIFO runs newest-first");
        }
    }

    @Test
    @Timeout(40)
    void fifoIsTheDefault(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("fifo.db").toString()).open()) {
            List<Integer> order = runAndRecord(queue, 6);
            assertEquals(List.of(0, 1, 2, 3, 4, 5), order, "FIFO runs oldest-first by default");
        }
    }
}
