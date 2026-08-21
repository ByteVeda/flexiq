package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.internal.JniQueueBackend;
import org.byteveda.flexiq.model.WorkerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A registered worker reports a fingerprint of the tasks it can run.
 *
 * <p>Discovery builds the registry at runtime, so a worker that registered part
 * of it looks healthy and dead-letters every job for the rest. The fingerprint
 * on the registry row is what makes that worker visible without going host by
 * host.
 */
class WorkerRegistryFingerprintTest {

    /**
     * The value {@code crates/flexiq-core/BINDING_CONTRACT.md} pins for this task set. Hard-coded
     * rather than recomputed here: a test that reimplemented the hash would agree with any drift in
     * it, and the reason the constant matters is that a Java worker and a worker in another SDK
     * have to produce the same string for the same registry.
     */
    private static final String INVOICES_AND_REPORTS = "fafd30ef8ebcb7de";

    @Test
    void recordsAFingerprintOfEveryRegisteredHandler(@TempDir Path dir) throws Exception {
        String options = new ObjectMapper()
                .writeValueAsString(
                        Map.of("backend", "sqlite", "dsn", dir.resolve("t.db").toString()));
        JniQueueBackend backend = JniQueueBackend.open(options);

        try (FlexiQ queue = FlexiQ.builder().open(backend)) {
            // Registered in the opposite order to the fingerprint's, to pin that
            // the value is over the set rather than over registration order.
            try (Worker worker = queue.worker()
                    .handle("reports.build", String.class, payload -> null)
                    .handle("invoices.send", String.class, payload -> null)
                    .start()) {
                WorkerInfo registered = queue.listWorkers().get(0);

                assertEquals(INVOICES_AND_REPORTS, registered.registryFingerprint);
            }
        }
    }
}
