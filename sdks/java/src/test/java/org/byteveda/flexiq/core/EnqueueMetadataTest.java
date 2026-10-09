package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.middleware.EnqueueContext;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A hook's metadata is layered over the caller's rather than replacing it. */
class EnqueueMetadataTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Task<String> TASK = Task.of("metadata.task", String.class);

    private static String stored(Path dir, String callerMetadata) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("m.db").toString()).open()) {
            queue.use(new Middleware() {
                @Override
                public void onEnqueue(EnqueueContext context) {
                    context.metadata().put("hook", "added");
                    context.metadata().put("shared", "hook");
                }
            });
            String id = queue.enqueue(
                    TASK,
                    "payload",
                    EnqueueOptions.builder().metadata(callerMetadata).build());
            return queue.getJob(id).orElseThrow().metadata;
        }
    }

    @Test
    void anObjectKeepsItsKeysAndTheHooksWinACollision(@TempDir Path dir) throws Exception {
        Map<?, ?> metadata = JSON.readValue(stored(dir, "{\"traceparent\":\"kept\",\"shared\":\"caller\"}"), Map.class);
        assertEquals(Map.of("traceparent", "kept", "hook", "added", "shared", "hook"), metadata);
    }

    @Test
    void metadataThatIsNotAnObjectIsReplaced(@TempDir Path dir) throws Exception {
        Map<?, ?> metadata = JSON.readValue(stored(dir, "free text"), Map.class);
        assertEquals(Map.of("hook", "added", "shared", "hook"), metadata);
    }

    @Test
    void theCallersDecimalsKeepTheirPrecision(@TempDir Path dir) {
        String stored = stored(dir, "{\"price\":1.10,\"exact\":0.123456789012345678901234567890}");
        assertTrue(stored.contains("\"price\":1.10"), stored);
        assertTrue(stored.contains("\"exact\":0.123456789012345678901234567890"), stored);
    }

    @Test
    void anArrayIsNotMistakenForAnObject(@TempDir Path dir) throws Exception {
        Map<?, ?> metadata = JSON.readValue(stored(dir, "[1,2]"), Map.class);
        assertEquals(Map.of("hook", "added", "shared", "hook"), metadata);
    }
}
