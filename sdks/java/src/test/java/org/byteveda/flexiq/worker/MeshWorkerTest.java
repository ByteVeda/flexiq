package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A mesh-enabled worker runs jobs end-to-end through the mesh bridge (solo node,
 * no peers) and exposes cluster info; a non-mesh worker reports no cluster.
 */
class MeshWorkerTest {

    private static final Task<String> ECHO = Task.of("mesh.echo", String.class);

    @Test
    @Timeout(60)
    void meshWorkerProcessesJobs(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("mesh.db").toString()).open()) {
            try (Worker worker = queue.worker()
                    .handle(ECHO, payload -> payload)
                    .mesh(MeshOptions.builder().port(17946).build())
                    .start()) {
                String id = queue.enqueue(ECHO, "via-mesh");
                queue.awaitJob(id, Duration.ofSeconds(30));
                assertEquals("via-mesh", queue.getResult(id, String.class).orElseThrow());

                MeshClusterInfo info = worker.meshClusterInfo().orElseThrow();
                assertEquals(0, info.peerCount(), "a solo node has no peers");
            }
        }
    }

    @Test
    @Timeout(60)
    void nonMeshWorkerHasNoClusterInfo(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("plain.db").toString()).open()) {
            try (Worker worker = queue.worker().handle(ECHO, payload -> payload).start()) {
                assertTrue(worker.meshClusterInfo().isEmpty());
            }
        }
    }

    @Test
    void rejectsOutOfRangePort() {
        assertThrows(IllegalArgumentException.class, () -> MeshOptions.builder().port(70000));
    }
}
