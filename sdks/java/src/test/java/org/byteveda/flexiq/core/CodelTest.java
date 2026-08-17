package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.DeadJob;
import org.byteveda.flexiq.model.QueueStats;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * S27 — opt-in CoDel load shedding. The controller algorithm is unit-tested in
 * Rust (`scheduler::codel`); this drives the end-to-end shed path with a slow
 * task at concurrency 1 so the queue backs up and stale jobs are shed to the DLQ.
 */
class CodelTest {

    @Test
    void codelValidatesBounds(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("v.db").toString()).open()) {
            assertThrows(IllegalArgumentException.class, () -> queue.codel("default", 0, 10));
            assertThrows(IllegalArgumentException.class, () -> queue.codel("default", 10, -1));
        }
    }

    @Test
    @Timeout(60)
    void shedsStaleJobsUnderOverload(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("codel.db").toString()).open()) {
            queue.codel("default", 1, 30);
            Task<String> slow = Task.of("slow", String.class);

            int total = 20;
            for (int i = 0; i < total; i++) {
                queue.enqueue(slow, "x");
            }

            Worker worker = queue.worker()
                    .concurrency(1)
                    .batchSize(1)
                    .handle(slow, payload -> {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    })
                    .start();
            try (worker) {
                // Poll until every job is accounted for and at least one was shed.
                long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
                long codelDead = 0;
                QueueStats stats = queue.stats();
                while (System.nanoTime() < deadline) {
                    List<DeadJob> dead = queue.listDead(100, 0);
                    codelDead = dead.stream()
                            .filter(d -> d.error != null && d.error.startsWith("codel:"))
                            .count();
                    stats = queue.stats();
                    if (stats.completed + stats.dead == total && codelDead >= 1) {
                        break;
                    }
                    Thread.sleep(100);
                }

                // A job can be shed between the dead-letter read and the stats
                // read that satisfied the exit condition; once converged nothing
                // moves, so a fresh dead-letter read gives the settled count.
                codelDead = queue.listDead(100, 0).stream()
                        .filter(d -> d.error != null && d.error.startsWith("codel:"))
                        .count();

                assertTrue(codelDead >= 1, "sustained overload should shed at least one stale job");
                assertEquals(codelDead, stats.dead, "every dead job is a CoDel drop");
                assertEquals(total, stats.completed + stats.dead, "no job is lost");
            }
        }
    }

    @Test
    @Timeout(60)
    void codelConfiguredAfterBuilderIsStillApplied(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("late.db").toString()).open()) {
            Task<String> slow = Task.of("slow", String.class);
            int total = 20;
            for (int i = 0; i < total; i++) {
                queue.enqueue(slow, "x");
            }

            // Obtain the builder BEFORE configuring CoDel, then configure it — the
            // late-bound queue-config source must pick this up at start().
            Worker.Builder builder = queue.worker().concurrency(1).batchSize(1).handle(slow, payload -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            });
            queue.codel("default", 1, 30);

            Worker worker = builder.start();
            try (worker) {
                long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
                long codelDead = 0;
                QueueStats stats = queue.stats();
                while (System.nanoTime() < deadline) {
                    codelDead = queue.listDead(100, 0).stream()
                            .filter(d -> d.error != null && d.error.startsWith("codel:"))
                            .count();
                    stats = queue.stats();
                    if (stats.completed + stats.dead == total && codelDead >= 1) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertTrue(codelDead >= 1, "CoDel set after the builder was obtained must still apply");
            }
        }
    }
}
