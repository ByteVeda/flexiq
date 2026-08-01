package org.byteveda.taskito.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.taskito.task.Task;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end tests for {@link Executor} against a socket speaking the frame
 * protocol, so they need no `taskito-server` build.
 *
 * <p>The wire is the contract: a job frame in, a result frame out. Asserting on
 * the frames rather than on storage is what makes these runnable anywhere.
 */
class ExecutorAttachTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int PROTOCOL_VERSION = 1;
    private static final long SETTLE_MS = 20_000;

    private @Nullable FakeScheduler scheduler;
    private @Nullable Executor executor;

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.close();
            executor = null;
        }
        if (scheduler != null) {
            scheduler.close();
            scheduler = null;
        }
    }

    /** The scheduler end of an attach, driven frame by frame. */
    private static final class FakeScheduler implements AutoCloseable {
        private final ServerSocket server;
        private final Thread accepting;
        private final CountDownLatch connected = new CountDownLatch(1);
        private final AtomicReference<@Nullable Socket> socket = new AtomicReference<>();
        private final AtomicReference<@Nullable JsonNode> hello = new AtomicReference<>();
        private final Deque<JsonNode> results = new ArrayDeque<>();
        private final boolean refuse;
        private @Nullable InputStream in;
        private @Nullable OutputStream out;

        FakeScheduler(boolean refuse) throws IOException {
            this.refuse = refuse;
            this.server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            this.accepting = new Thread(this::accept, "fake-scheduler");
            this.accepting.setDaemon(true);
            this.accepting.start();
        }

        int port() {
            return server.getLocalPort();
        }

        private void accept() {
            try {
                Socket client = server.accept();
                socket.set(client);
                in = client.getInputStream();
                out = client.getOutputStream();
                JsonNode frame = readFrame();
                hello.set(frame);
                if (refuse) {
                    client.close();
                } else {
                    send(Map.of(
                            "type",
                            "hello_ack",
                            "scheduler_id",
                            "fake-scheduler",
                            "protocol_version",
                            PROTOCOL_VERSION));
                }
                connected.countDown();
            } catch (IOException e) {
                connected.countDown();
            }
        }

        /** Read one frame: a JSON header line, then exactly the payload it declares. */
        private @Nullable JsonNode readFrame() throws IOException {
            InputStream stream = in;
            if (stream == null) {
                return null;
            }
            ByteArrayOutputStream header = new ByteArrayOutputStream();
            int b;
            while ((b = stream.read()) != -1 && b != '\n') {
                header.write(b);
            }
            if (header.size() == 0) {
                return null;
            }
            JsonNode node = JSON.readTree(header.toString(StandardCharsets.UTF_8));
            int declared = declaredPayloadLength(node);
            if (declared > 0) {
                stream.readNBytes(declared);
            }
            return node;
        }

        private static int declaredPayloadLength(JsonNode header) {
            String type = header.path("type").asText("");
            if (type.equals("job")) {
                return header.path("payload_len").asInt(0);
            }
            if (type.equals("success")) {
                JsonNode len = header.get("result_len");
                return len == null || len.isNull() ? 0 : len.asInt(0);
            }
            return 0;
        }

        JsonNode awaitHello() throws InterruptedException {
            assertTrue(connected.await(SETTLE_MS, TimeUnit.MILLISECONDS), "the executor never attached");
            JsonNode frame = hello.get();
            assertNotNull(frame, "no hello frame arrived");
            return frame;
        }

        void send(Map<String, Object> header) throws IOException {
            OutputStream stream = out;
            if (stream == null) {
                return;
            }
            stream.write(JSON.writeValueAsBytes(header));
            stream.write('\n');
            stream.flush();
        }

        void sendJob(String id, String taskName, byte[] payload) throws IOException {
            OutputStream stream = out;
            if (stream == null) {
                return;
            }
            Map<String, Object> header = Map.of(
                    "type",
                    "job",
                    "id",
                    id,
                    "task_name",
                    taskName,
                    "payload_len",
                    payload.length,
                    "retry_count",
                    0,
                    "max_retries",
                    3,
                    "queue",
                    "default",
                    "timeout_ms",
                    30_000);
            stream.write(JSON.writeValueAsBytes(header));
            stream.write('\n');
            stream.write(payload);
            stream.flush();
        }

        /** The next frame that is not a heartbeat. */
        JsonNode nextResult() throws IOException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SETTLE_MS);
            while (System.nanoTime() < deadline) {
                if (!results.isEmpty()) {
                    return results.removeFirst();
                }
                JsonNode frame = readFrame();
                if (frame == null) {
                    break;
                }
                if (!frame.path("type").asText("").equals("heartbeat")) {
                    return frame;
                }
            }
            throw new AssertionError("no result frame arrived");
        }

        @Override
        public void close() throws IOException {
            Socket client = socket.get();
            if (client != null) {
                client.close();
            }
            server.close();
            accepting.interrupt();
        }
    }

    private Executor attach(Executor.Builder builder, int port) {
        Executor started = builder.attach("127.0.0.1:" + port)
                .heartbeatIntervalMs(50)
                .shutdownDrainMs(5_000)
                .start();
        executor = started;
        return started;
    }

    private static Executor.Builder greeter() {
        Task<String> greet = Task.of("greet", String.class);
        return Executor.builder().register(Handler.of(greet, (String who) -> "hello " + who));
    }

    @Test
    @Timeout(60)
    void announcesItselfAndTheTasksItCanRun() throws Exception {
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        attach(greeter().executorId("exec-test").slots(2), fake.port());
        JsonNode hello = fake.awaitHello();

        assertEquals("exec-test", hello.path("executor_id").asText());
        assertEquals("java", hello.path("sdk").asText());
        assertEquals(2, hello.path("slots").asInt());
        assertEquals(PROTOCOL_VERSION, hello.path("protocol_version").asInt());
        // Only advertised tasks are ever dispatched, so a missing name here is a
        // job that silently never runs.
        assertEquals("greet", hello.path("tasks").get(0).asText());
        // A token that was never configured must not appear on the wire.
        assertTrue(hello.get("token") == null || hello.get("token").isNull());
    }

    @Test
    @Timeout(60)
    void runsADispatchedJobAndReturnsItsResult() throws Exception {
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        attach(greeter(), fake.port());
        fake.awaitHello();
        fake.sendJob("job-1", "greet", JSON.writeValueAsBytes("ada"));

        JsonNode result = fake.nextResult();
        assertEquals("success", result.path("type").asText());
        assertEquals("job-1", result.path("job_id").asText());
    }

    @Test
    @Timeout(60)
    void reportsAFailureWithItsRetryVerdict() throws Exception {
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        Task<String> boom = Task.of("boom", String.class);
        attach(
                Executor.builder().register(Handler.of(boom, (String ignored) -> {
                    throw new IllegalStateException("deliberate failure");
                })),
                fake.port());
        fake.awaitHello();
        fake.sendJob("job-1", "boom", JSON.writeValueAsBytes("x"));

        JsonNode result = fake.nextResult();
        assertEquals("failure", result.path("type").asText());
        assertEquals("job-1", result.path("job_id").asText());
        assertTrue(result.path("should_retry").asBoolean(), "an unclassified failure is retryable");
        assertFalse(result.path("timed_out").asBoolean());
        assertTrue(result.path("error").asText().contains("deliberate failure"));
    }

    @Test
    @Timeout(60)
    void aShutdownFrameEndsTheSession() throws Exception {
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        Executor running = attach(greeter(), fake.port());
        fake.awaitHello();
        assertTrue(running.isRunning());

        fake.send(Map.of("type", "shutdown"));
        running.awaitSession();
        assertFalse(running.isRunning(), "a shutdown frame must end the session");
    }

    @Test
    @Timeout(60)
    void stopReleasesAParkedWaiter() throws Exception {
        // `stop()` cannot unpark the frame reader, so the session has to end
        // locally too — otherwise a shutdown hook that stops and then waits
        // would hang instead of draining.
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        Executor running = attach(greeter(), fake.port());
        fake.awaitHello();

        Thread waiter = new Thread(running::awaitSession, "await-session");
        waiter.setDaemon(true);
        waiter.start();

        running.stop();
        waiter.join(SETTLE_MS);
        assertFalse(waiter.isAlive(), "stop() never released the waiter");
    }

    @Test
    @Timeout(60)
    void aRefusedAttachIsReportedRatherThanRetried() throws Exception {
        FakeScheduler fake = new FakeScheduler(true);
        scheduler = fake;

        // A wrong token is the likeliest deployment mistake; it must not surface
        // as a bare network error.
        RuntimeException error = assertThrows(RuntimeException.class, () -> greeter()
                .attach("127.0.0.1:" + fake.port())
                .token("wrong-token")
                .start());
        assertTrue(
                error.getMessage().toLowerCase(java.util.Locale.ROOT).contains("refused")
                        || error.getMessage().toLowerCase(java.util.Locale.ROOT).contains("token"),
                "expected a refusal, got: " + error.getMessage());
    }

    @Test
    @Timeout(60)
    void anAddressIsRequired() {
        IllegalStateException error =
                assertThrows(IllegalStateException.class, () -> greeter().start());
        assertTrue(error.getMessage().contains("TASKITO_ATTACH"), error.getMessage());
    }

    @Test
    @Timeout(60)
    void handlersAreRequired() {
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> Executor.builder().attach("127.0.0.1:1").start());
        assertTrue(error.getMessage().contains("no handlers"), error.getMessage());
    }

    @Test
    @Timeout(60)
    void anUnreachableSchedulerFailsFast() {
        // Port 1 on loopback is reserved and nothing listens there.
        RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> greeter().attach("127.0.0.1:1").connectTimeoutMs(500).start());
        assertTrue(error.getMessage().contains("could not reach the scheduler"), error.getMessage());
    }
}
