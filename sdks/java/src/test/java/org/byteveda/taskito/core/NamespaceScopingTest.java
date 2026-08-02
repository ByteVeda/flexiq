package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A namespace is a tenancy boundary: a caller scoped to one must learn nothing
 * about ids outside it — not through a read, and not through the effect of a
 * write. An unset namespace stays unscoped and addresses every namespace.
 */
class NamespaceScopingTest {

    private static Taskito open(Path db, String namespace) {
        Taskito.Builder builder = Taskito.builder().backend("sqlite").url(db.toString());
        if (namespace != null) {
            builder = builder.namespace(namespace);
        }
        return builder.open();
    }

    @Test
    @Timeout(30)
    void scopesTheIdAddressedSurface(@TempDir Path dir) {
        Path db = dir.resolve("t.db");
        Task<String> work = Task.of("work", String.class);

        try (Taskito a = open(db, "ns-a");
                Taskito b = open(db, "ns-b");
                Taskito unscoped = open(db, null)) {
            String id = a.enqueue(work, "payload");

            assertTrue(a.getJob(id).isPresent(), "the owning namespace sees its job");
            assertTrue(b.getJob(id).isEmpty(), "another namespace reads it as missing");
            assertTrue(unscoped.getJob(id).isPresent(), "unscoped addresses every namespace");

            assertFalse(b.cancel(id), "a cross-namespace cancel reports like an unknown id");
            assertFalse(b.requestCancel(id));
            assertTrue(a.getJob(id).isPresent(), "and must not have landed");

            assertEquals(0, b.jobErrors(id).size());
            assertEquals(0, b.getTaskLogs(id).size());

            assertTrue(a.cancel(id), "the owning namespace still cancels");
        }
    }
}
