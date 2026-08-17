package org.byteveda.flexiq.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end payload fidelity through the active transport (FFM on JDK 22+, else
 * JNI): tricky payloads must survive enqueue → store → worker → getResult
 * unchanged, including empty, full-Unicode, and large payloads that stress the
 * byte transfer. Also covers the batch (enqueueMany) path.
 */
class FfmRoundTripTest {

    private static final Task<String> ECHO = Task.of("ffm.echo", String.class);

    @Test
    @Timeout(60)
    void payloadsRoundTrip(@TempDir Path dir) throws Exception {
        String unicode = "héllo 🌍 \t\n  quotes \" backslash \\ end";
        String large = "x".repeat(256 * 1024);
        List<String> payloads = List.of("", unicode, large);

        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("ffm.db").toString()).open()) {
            Worker worker = queue.worker().handle(ECHO, p -> p).start();
            try (worker) {
                for (String payload : payloads) {
                    String id = queue.enqueue(ECHO, payload);
                    queue.awaitJob(id, Duration.ofSeconds(30));
                    assertEquals(payload, queue.getResult(id, String.class).orElseThrow());
                }
            }
        }
    }

    @Test
    @Timeout(60)
    void batchRoundTrips(@TempDir Path dir) throws Exception {
        List<String> payloads = List.of("alpha", "", "gamma 🌍");

        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("ffm-batch.db").toString()).open()) {
            Worker worker = queue.worker().handle(ECHO, p -> p).start();
            try (worker) {
                List<String> ids = queue.enqueueMany(ECHO, payloads);
                assertEquals(payloads.size(), ids.size());
                for (int i = 0; i < ids.size(); i++) {
                    queue.awaitJob(ids.get(i), Duration.ofSeconds(30));
                    assertEquals(
                            payloads.get(i),
                            queue.getResult(ids.get(i), String.class).orElseThrow());
                }
            }
        }
    }
}
