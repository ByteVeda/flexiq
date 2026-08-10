package org.byteveda.taskito;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.Map;
import org.byteveda.taskito.internal.JniQueueBackend;
import org.byteveda.taskito.model.MigrationReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A deployment that gates DDL opens unmigrated and applies the schema itself.
 *
 * <p>Until migrate has run there are no tables, so every query fails — that is
 * the gate working, not a fault.
 */
class MigrateTest {

    private static String optionsFor(Path dir, boolean autoMigrate) throws Exception {
        return new ObjectMapper()
                .writeValueAsString(
                        Map.of("backend", "sqlite", "dsn", dir.resolve("t.db").toString(), "autoMigrate", autoMigrate));
    }

    @Test
    void anUnmigratedQueueAppliesItsOwnSchema(@TempDir Path dir) throws Exception {
        try (Taskito queue = Taskito.builder().open(JniQueueBackend.open(optionsFor(dir, false)))) {
            assertThrows(Exception.class, queue::stats, "no tables exist yet");

            MigrationReport report = queue.migrate();
            assertFalse(report.applied.isEmpty(), "the first migrate applies the whole history");
            assertFalse(report.schemaless);
            queue.stats();

            assertTrue(queue.migrate().isEmpty(), "a current database reports no work");
        }
    }

    @Test
    void anAutoMigratedQueueHasOnlyItsWorkflowTablesLeft(@TempDir Path dir) throws Exception {
        try (Taskito queue = Taskito.builder().open(JniQueueBackend.open(optionsFor(dir, true)))) {
            // Opening applies the core schema; the workflow tables are built on
            // first workflow use, so an explicit migrate is what brings them
            // forward for a deployment that wants no DDL at runtime.
            MigrationReport report = queue.migrate();
            assertTrue(report.applied.isEmpty(), "the core schema was applied at open");
            assertFalse(report.workflowApplied.isEmpty(), "workflow tables were still pending");

            assertTrue(queue.migrate().isEmpty(), "a second run has nothing left");
        }
    }
}
