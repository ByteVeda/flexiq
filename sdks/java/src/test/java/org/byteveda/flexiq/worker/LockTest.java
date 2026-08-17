package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.FlexiQException;
import org.byteveda.flexiq.locks.Lock;
import org.byteveda.flexiq.scheduling.PeriodicTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LockTest {

    private FlexiQ open(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open();
    }

    @Test
    void acquireContendRelease(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            Lock first = queue.lock("L", 60_000);
            assertTrue(first.acquire());
            assertFalse(queue.lock("L", 60_000).acquire());
            assertTrue(queue.lockInfo("L").isPresent());

            first.release();
            assertFalse(queue.lockInfo("L").isPresent());

            boolean[] ran = {false};
            assertTrue(queue.withLock("M", 30_000, () -> ran[0] = true));
            assertTrue(ran[0]);
        }
    }

    @Test
    void registerPeriodicValidatesCron(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            long before = System.currentTimeMillis();
            long next = queue.registerPeriodic(
                    PeriodicTask.builder("p", "tick", "0 0 12 * * *").build());
            // Next firing must be strictly after the pre-registration instant.
            // Comparing to a later wall-clock read could flake at the cron boundary.
            assertTrue(next > before);

            assertThrows(
                    FlexiQException.class,
                    () -> queue.registerPeriodic(
                            PeriodicTask.builder("p", "tick", "not a cron").build()));
        }
    }
}
