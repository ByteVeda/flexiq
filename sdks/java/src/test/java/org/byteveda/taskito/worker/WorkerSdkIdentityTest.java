package org.byteveda.taskito.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.Map;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.internal.JniQueueBackend;
import org.byteveda.taskito.model.WorkerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A registered worker reports which SDK and release it runs.
 *
 * <p>In a polyglot deployment the registry is the only place an operator can
 * tell a stale worker from a current one without going host by host.
 */
class WorkerSdkIdentityTest {

    @Test
    void recordsTheSdkAndItsVersionOnRegistration(@TempDir Path dir) throws Exception {
        String options = new ObjectMapper()
                .writeValueAsString(
                        Map.of("backend", "sqlite", "dsn", dir.resolve("t.db").toString()));
        JniQueueBackend backend = JniQueueBackend.open(options);

        try (Taskito queue = Taskito.builder().open(backend)) {
            try (Worker worker = queue.worker().start()) {
                WorkerInfo registered = queue.listWorkers().get(0);

                assertEquals("java", registered.sdk);
                // The version is stamped from the native library's crate
                // version, so assert its shape rather than a literal that a
                // release bump would invalidate.
                assertNotNull(registered.sdkVersion, "no sdk_version recorded");
                assertTrue(
                        registered.sdkVersion.matches("\\d+\\.\\d+\\.\\d+.*"),
                        "unexpected version shape: " + registered.sdkVersion);
            }
        }
    }
}
