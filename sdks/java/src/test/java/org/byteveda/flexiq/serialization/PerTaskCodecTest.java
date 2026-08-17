package org.byteveda.flexiq.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.InterceptionException;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.interception.Interception;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class PerTaskCodecTest {

    private static final byte[] KEY32 = "0123456789abcdef0123456789abcdef".getBytes();

    @Test
    @Timeout(30)
    void programmaticPerTaskCodecRoundTrips(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder()
                .url(dir.resolve("ptc.db").toString())
                .codec("secret", new AesGcmCodec(KEY32))
                .open()) {
            Task<String> task = Task.of("ptc.echo", String.class).codecs("secret");
            AtomicReference<String> seen = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(task, p -> {
                        seen.set(p);
                        ran.countDown();
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(task, "hello");
                assertTrue(ran.await(20, TimeUnit.SECONDS));
                assertEquals("hello", seen.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void annotatedTaskUsesEncryptedCodec(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder()
                .url(dir.resolve("eg.db").toString())
                .codec("encrypted", new AesGcmCodec(KEY32))
                .open()) {
            // EncryptedGreeterTasks.GREET is generated with .codecs("encrypted").
            String id = queue.enqueue(EncryptedGreeterTasks.GREET, "ada");
            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .apply(builder -> EncryptedGreeterTasks.bind(builder, new EncryptedGreeter()))
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS));
                assertEquals("secret ada", queue.getResult(id, String.class).orElseThrow());
            }
        }
    }

    @Test
    void annotatedTaskRequiresRegisteredCodec(@TempDir Path dir) {
        // No "encrypted" codec registered, so the generated .codecs("encrypted") fails the enqueue —
        // proving the annotation wired the codec onto the task.
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("ar.db").toString()).open()) {
            assertThrows(IllegalStateException.class, () -> queue.enqueue(EncryptedGreeterTasks.GREET, "ada"));
        }
    }

    @Test
    void redirectingACodecTaskIsRejected(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder()
                .url(dir.resolve("rc.db").toString())
                .codec("secret", new AesGcmCodec(KEY32))
                .open()) {
            queue.intercept((task, payload) -> Interception.redirect("other", payload));
            Task<String> task = Task.of("rc.echo", String.class).codecs("secret");
            // Redirect can't carry the source task's codec chain to a different task — fail fast.
            assertThrows(InterceptionException.class, () -> queue.enqueue(task, "hello"));
        }
    }
}
