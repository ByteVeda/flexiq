package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.PeriodicInfo;
import org.byteveda.flexiq.scheduling.PeriodicTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Periodic CRUD over the JNI backend: list / pause-resume (toggles enabled) / delete. */
class PeriodicTest {

    private static PeriodicInfo find(List<PeriodicInfo> tasks, String name) {
        Optional<PeriodicInfo> match =
                tasks.stream().filter(p -> p.name.equals(name)).findFirst();
        assertTrue(match.isPresent(), "expected periodic task '" + name + "'");
        return match.get();
    }

    @Test
    @Timeout(30)
    void listPauseResumeDelete(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.registerPeriodic(
                    PeriodicTask.builder("nightly", "report", "0 0 0 * * *").build());
            queue.registerPeriodic(
                    PeriodicTask.builder("hourly", "sync", "0 0 * * * *").build());

            assertEquals(2, queue.listPeriodic().size());
            assertTrue(find(queue.listPeriodic(), "nightly").enabled);

            // Pause toggles enabled but keeps the task in the catalog.
            assertTrue(queue.pausePeriodic("nightly"));
            assertFalse(find(queue.listPeriodic(), "nightly").enabled);
            assertEquals(2, queue.listPeriodic().size());

            assertTrue(queue.resumePeriodic("nightly"));
            assertTrue(find(queue.listPeriodic(), "nightly").enabled);

            // Unknown name → not found.
            assertFalse(queue.pausePeriodic("ghost"));

            // Delete removes it; a second delete reports not-found.
            assertTrue(queue.deletePeriodic("nightly"));
            assertEquals(1, queue.listPeriodic().size());
            assertFalse(queue.deletePeriodic("nightly"));
        }
    }
}
