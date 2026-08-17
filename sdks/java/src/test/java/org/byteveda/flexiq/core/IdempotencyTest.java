package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.internal.IdempotencyKeys;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class IdempotencyTest {

    // Golden vectors — the auto-key recipe is a cross-SDK wire contract, so these must match
    // the values a peer SDK produces from sha256(utf8(name) ++ 0x00 ++ payload)[:32].
    @Test
    void autoKeyMatchesCrossSdkGoldenVectors() {
        assertEquals(
                "auto:ca3f9aac3860dab1f1edb773c39ca21e",
                IdempotencyKeys.autoKey("send_email", "{\"to\":\"a@b.com\"}".getBytes(StandardCharsets.UTF_8)));
        assertEquals("auto:ff688985bc4fd8bdde332c05f8cb0d86", IdempotencyKeys.autoKey("task", new byte[0]));
        assertEquals(
                "auto:6b70352f019e763745b932f8330fe74e",
                IdempotencyKeys.autoKey("send_email", "hello".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void autoKeyHasStableShape() {
        String key = IdempotencyKeys.autoKey("t", "p".getBytes(StandardCharsets.UTF_8));
        assertEquals("auto:".length() + 32, key.length());
    }

    @Test
    @Timeout(30)
    void idempotentTaskDedupesWhilePending(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("idem.db").toString()).open()) {
            Task<String> task = Task.of("idem.dedup", String.class).idempotent(true);
            // No worker: both jobs stay pending, so the second enqueue dedups to the first.
            String first = queue.enqueue(task, "hello");
            String second = queue.enqueue(task, "hello");
            assertEquals(first, second, "same payload should dedup to the same job");

            String other = queue.enqueue(task, "world");
            assertNotEquals(first, other, "a different payload derives a different key");
        }
    }

    @Test
    @Timeout(30)
    void explicitIdempotencyKeyWinsOverPayload(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("idemkey.db").toString()).open()) {
            Task<String> task = Task.of("idem.key", String.class);
            EnqueueOptions opts = EnqueueOptions.builder().idempotencyKey("k1").build();
            // Different payloads but the same explicit key dedup together.
            String first = queue.enqueue(task, "a", opts);
            String second = queue.enqueue(task, "b", opts);
            assertEquals(first, second);
        }
    }

    @Test
    @Timeout(30)
    void enqueueManyHonorsIdempotentAutoDerive(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("idembatch.db").toString()).open()) {
            Task<String> task = Task.of("idem.batch", String.class).idempotent(true);
            // No worker: jobs stay pending. The batch-enqueued payload must carry the
            // auto-derived key so a later single enqueue of the same payload dedups —
            // regressing when enqueueMany skipped resolveUniqueKey.
            List<String> ids = queue.enqueueMany(task, List.of("hello", "world"));
            String again = queue.enqueue(task, "hello");
            assertEquals(ids.get(0), again, "batch enqueue must auto-derive the dedup key");
            assertNotEquals(ids.get(1), again, "a different payload derives a different key");
        }
    }

    @Test
    @Timeout(30)
    void perCallOptOutDisablesTaskDefault(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("optout.db").toString()).open()) {
            Task<String> task = Task.of("idem.optout", String.class).idempotent(true);
            EnqueueOptions off = EnqueueOptions.builder().idempotent(false).build();
            String first = queue.enqueue(task, "hello", off);
            String second = queue.enqueue(task, "hello", off);
            assertNotEquals(first, second, "idempotent(false) opts out of the task default");
        }
    }
}
