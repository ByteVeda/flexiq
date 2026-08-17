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
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.taskito.JobContext;
import org.byteveda.taskito.middleware.Middleware;
import org.byteveda.taskito.middleware.TaskContext;
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
        private final Deque<Frame> results = new ArrayDeque<>();
        private final boolean refuse;

        /**
         * What this scheduler promises to do on the executor's behalf. Empty by
         * default — a scheduler built before the side-channel existed, which is
         * the compatibility case worth defaulting to.
         */
        private final List<String> capabilities;

        private @Nullable InputStream in;
        private @Nullable OutputStream out;

        FakeScheduler(boolean refuse) throws IOException {
            this(refuse, List.of());
        }

        FakeScheduler(boolean refuse, List<String> capabilities) throws IOException {
            this.refuse = refuse;
            this.capabilities = capabilities;
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
                Frame frame = readFrame();
                hello.set(frame == null ? null : frame.header());
                if (refuse) {
                    client.close();
                } else {
                    send(Map.of(
                            "type",
                            "hello_ack",
                            "scheduler_id",
                            "fake-scheduler",
                            "protocol_version",
                            PROTOCOL_VERSION,
                            "capabilities",
                            capabilities));
                }
                connected.countDown();
            } catch (IOException e) {
                connected.countDown();
            }
        }

        /** A frame header and the blob that followed it. */
        record Frame(JsonNode header, byte[] payload) {
            String type() {
                return header.path("type").asText("");
            }
        }

        /** Read one frame: a JSON header line, then exactly the payload it declares. */
        private @Nullable Frame readFrame() throws IOException {
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
            byte[] payload = declared > 0 ? stream.readNBytes(declared) : new byte[0];
            return new Frame(node, payload);
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
            if (type.equals("task_log")) {
                // A published partial can be arbitrarily large, so `extra` rides
                // as the frame's blob rather than inside the header.
                JsonNode len = header.get("extra_len");
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
            sendJob(id, taskName, payload, List.of(), null);
        }

        void sendJob(
                String id,
                String taskName,
                byte[] payload,
                List<String> disabledMiddleware,
                @Nullable String metadataJson)
                throws IOException {
            OutputStream stream = out;
            if (stream == null) {
                return;
            }
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("type", "job");
            header.put("id", id);
            header.put("task_name", taskName);
            header.put("payload_len", payload.length);
            header.put("retry_count", 0);
            header.put("max_retries", 3);
            header.put("queue", "default");
            header.put("timeout_ms", 30_000);
            // Resolved by the scheduler, because an executor has no storage of
            // its own to read the settings or the job row from.
            header.put("disabled_middleware", disabledMiddleware);
            header.put("metadata", metadataJson);
            stream.write(JSON.writeValueAsBytes(header));
            stream.write('\n');
            stream.write(payload);
            stream.flush();
        }

        /**
         * Every side-channel frame a job produced, plus its result.
         *
         * <p>The result is ordered behind them on one connection, so its arrival
         * is what proves the collection is complete rather than merely early.
         */
        List<Frame> collectUntilResult() throws IOException {
            List<Frame> collected = new ArrayList<>();
            for (; ; ) {
                Frame frame = nextFrame();
                collected.add(frame);
                if (!frame.type().equals("progress") && !frame.type().equals("task_log")) {
                    return collected;
                }
            }
        }

        /** The next frame that is not a heartbeat. */
        JsonNode nextResult() throws IOException {
            return nextFrame().header();
        }

        Frame nextFrame() throws IOException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SETTLE_MS);
            while (System.nanoTime() < deadline) {
                if (!results.isEmpty()) {
                    return results.removeFirst();
                }
                Frame frame = readFrame();
                if (frame == null) {
                    break;
                }
                if (!frame.type().equals("heartbeat")) {
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

    /** A handler that uses the job-scoped conveniences needing storage in a worker. */
    private static Executor.Builder reporter() {
        Task<String> report = Task.of("report", String.class);
        return Executor.builder().register(Handler.of(report, (String ignored) -> {
            JobContext job = JobContext.current();
            job.setProgress(50);
            job.log("halfway");
            job.publish(Map.of("stage", "halfway"));
            job.setProgress(100);
            return "reported";
        }));
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
    void sendsProgressAndLogsToASchedulerThatAdvertisedTheSideChannel() throws Exception {
        // The whole point of #589: a task on an executor is not silently poorer
        // than the same task on an in-process worker.
        FakeScheduler fake = new FakeScheduler(false, List.of("side_channel"));
        scheduler = fake;

        attach(reporter(), fake.port());
        fake.awaitHello();
        fake.sendJob("job-1", "report", JSON.writeValueAsBytes("x"));

        List<FakeScheduler.Frame> frames = fake.collectUntilResult();
        assertEquals("success", frames.get(frames.size() - 1).type());

        List<Integer> progress = frames.stream()
                .filter(frame -> frame.type().equals("progress"))
                .map(frame -> frame.header().path("progress").asInt())
                .toList();
        assertFalse(progress.isEmpty(), "the task's progress must reach the scheduler");
        assertEquals(100, progress.get(progress.size() - 1));

        FakeScheduler.Frame partial = frames.stream()
                .filter(frame -> frame.header().path("level").asText("").equals("result"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the published partial never arrived"));
        assertEquals("job-1", partial.header().path("job_id").asText());
        assertEquals("report", partial.header().path("task_name").asText());
        assertEquals("halfway", JSON.readTree(partial.payload()).path("stage").asText());
    }

    @Test
    @Timeout(60)
    void sendsNothingToASchedulerThatAdvertisedNoSideChannel() throws Exception {
        // The negotiation path: an executor must never write a frame its peer
        // could not parse, so it degrades to dropping instead.
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        attach(reporter(), fake.port());
        fake.awaitHello();
        fake.sendJob("job-1", "report", JSON.writeValueAsBytes("x"));

        List<FakeScheduler.Frame> frames = fake.collectUntilResult();
        assertEquals(1, frames.size(), "only the result may cross the wire");
        assertEquals("success", frames.get(0).type());
    }

    @Test
    @Timeout(60)
    void skipsAMiddlewareTheDispatchSaysIsDisabled() throws Exception {
        // A dashboard toggle has to reach a process that cannot read settings,
        // so it rides the job frame instead.
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        List<String> ran = new CopyOnWriteArrayList<>();
        Middleware recorder = new Middleware() {
            @Override
            public void before(TaskContext context) {
                ran.add("recorder");
            }
        };
        attach(greeter().middleware(List.of(recorder)), fake.port());
        fake.awaitHello();

        fake.sendJob("job-1", "greet", JSON.writeValueAsBytes("ada"));
        assertEquals("success", fake.nextResult().path("type").asText());
        assertEquals(List.of("recorder"), ran);

        fake.sendJob(
                "job-2",
                "greet",
                JSON.writeValueAsBytes("bob"),
                List.of(recorder.getClass().getName()),
                null);
        assertEquals("success", fake.nextResult().path("type").asText());
        assertEquals(List.of("recorder"), ran, "a middleware disabled on the dispatch must not run");
    }

    @Test
    @Timeout(60)
    void middlewareReadsMetadataOffTheDispatch() throws Exception {
        // An executor cannot fetch the job row, so metadata rides the frame or
        // middleware sees an empty map.
        FakeScheduler fake = new FakeScheduler(false);
        scheduler = fake;

        AtomicReference<@Nullable Object> seen = new AtomicReference<>();
        Middleware reader = new Middleware() {
            @Override
            public void before(TaskContext context) {
                seen.set(context.job().metadata().get("trace_id"));
            }
        };
        attach(greeter().middleware(List.of(reader)), fake.port());
        fake.awaitHello();

        fake.sendJob("job-1", "greet", JSON.writeValueAsBytes("ada"), List.of(), "{\"trace_id\":\"abc\"}");
        assertEquals("success", fake.nextResult().path("type").asText());
        assertEquals("abc", seen.get());
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
        assertTrue(error.getMessage().contains("FLEXIQ_ATTACH"), error.getMessage());
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
