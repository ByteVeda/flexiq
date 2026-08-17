package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Duration;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code pushDispatch(true)} swaps the scheduler's poll loop for enqueue-driven
 * wakeups when the native library carries the {@code push-dispatch} cargo
 * feature, and is accepted and ignored (polling kept) when it doesn't. Either
 * way jobs must run to completion, so these cover both builds.
 */
class PushDispatchWorkerTest {

    private static final Task<String> ECHO = Task.of("push.echo", String.class);

    @Test
    @Timeout(60)
    void pushDispatchWorkerProcessesJobs(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("push.db").toString()).open()) {
            Worker worker = queue.worker()
                    .handle(ECHO, payload -> payload)
                    .pushDispatch(true)
                    .start();
            try (worker) {
                String id = queue.enqueue(ECHO, "via-push");
                queue.awaitJob(id, Duration.ofSeconds(30));
                assertEquals("via-push", queue.getResult(id, String.class).orElseThrow());
            }
        }
    }

    /**
     * A job enqueued before the worker exists has no wake to ride on. The fallback
     * timer (push builds) and the poll loop (default builds) must both still claim
     * it, so an early enqueue can never strand.
     */
    @Test
    @Timeout(60)
    void dispatchesWorkEnqueuedBeforeTheWorkerStarted(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("push-pre.db").toString()).open()) {
            String id = queue.enqueue(ECHO, "before-start");
            Worker worker = queue.worker()
                    .handle(ECHO, payload -> payload)
                    .pushDispatch(true)
                    .start();
            try (worker) {
                queue.awaitJob(id, Duration.ofSeconds(30));
                assertEquals("before-start", queue.getResult(id, String.class).orElseThrow());
            }
        }
    }
}
