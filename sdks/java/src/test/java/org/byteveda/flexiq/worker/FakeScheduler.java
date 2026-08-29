package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/**
 * The scheduler end of an attach, driven frame by frame.
 *
 * <p>Shared by every test that needs a scheduler without building one: the
 * executor speaks a socket protocol, so a plain {@link ServerSocket} is enough
 * to drive one end of it. Asserting on the frames rather than on storage is
 * what makes these tests runnable anywhere.
 *
 * <p>A frame is a JSON header line followed by exactly the number of raw
 * payload bytes it declares, so decoding has to be length-driven rather than
 * line-driven — a payload can contain newlines.
 */
public final class FakeScheduler implements AutoCloseable {
    /** Frame-format version this build speaks; mirrored from the core. */
    public static final int PROTOCOL_VERSION = 1;

    /** How long a test waits for a frame before calling it a failure. */
    public static final long SETTLE_MS = 20_000;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final ServerSocket server;
    private final Thread accepting;
    private final CountDownLatch connected = new CountDownLatch(1);
    private final AtomicReference<@Nullable Socket> socket = new AtomicReference<>();
    private final AtomicReference<@Nullable JsonNode> hello = new AtomicReference<>();

    /** Frames read while looking for one of another type, kept in arrival order. */
    private final Deque<Frame> pending = new ArrayDeque<>();

    private final boolean refuse;

    /**
     * What this scheduler promises to do on the executor's behalf. Empty by
     * default — a scheduler built before capabilities existed, which is the
     * compatibility case worth defaulting to.
     */
    private final List<String> capabilities;

    private @Nullable InputStream in;
    private @Nullable OutputStream out;

    /**
     * A scheduler that acknowledges the handshake and advertises nothing.
     *
     * @param refuse whether to drop the connection instead of acknowledging it
     * @throws IOException if the listening socket cannot be opened
     */
    public FakeScheduler(boolean refuse) throws IOException {
        this(refuse, List.of());
    }

    /**
     * A scheduler advertising {@code capabilities} in its {@code hello_ack}.
     *
     * @param refuse whether to drop the connection instead of acknowledging it
     * @param capabilities what this scheduler performs on the executor's behalf
     * @throws IOException if the listening socket cannot be opened
     */
    public FakeScheduler(boolean refuse, List<String> capabilities) throws IOException {
        this.refuse = refuse;
        this.capabilities = capabilities;
        this.server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        this.accepting = new Thread(this::accept, "fake-scheduler");
        this.accepting.setDaemon(true);
        this.accepting.start();
    }

    /**
     * The port this scheduler is listening on.
     *
     * @return the loopback port to attach an executor to
     */
    public int port() {
        return server.getLocalPort();
    }

    private void accept() {
        try {
            Socket client = server.accept();
            // A read that never returns cannot be interrupted, and JUnit's
            // @Timeout can only interrupt: without this a test whose frame never
            // arrives hangs the whole build instead of failing.
            client.setSoTimeout((int) SETTLE_MS);
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

    /**
     * A frame header and the blob that followed it.
     *
     * @param header the frame's JSON header line
     * @param payload the raw bytes the header declared, empty when it declared none
     */
    public record Frame(JsonNode header, byte[] payload) {
        /**
         * The frame's discriminator.
         *
         * @return the {@code type} field, or the empty string when absent
         */
        public String type() {
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

    /**
     * How many raw bytes follow this header.
     *
     * <p>Read by <b>field</b> rather than by frame type, the way the core's own
     * preamble does. A per-type switch answers zero for any frame added later —
     * and a frame whose blob is skipped desyncs the stream for good, because the
     * next header is then read out of the middle of a payload.
     */
    private static int declaredPayloadLength(JsonNode header) {
        for (String field : List.of("payload_len", "result_len", "extra_len")) {
            JsonNode length = header.get(field);
            if (length != null && !length.isNull()) {
                return length.asInt(0);
            }
        }
        return 0;
    }

    /**
     * The {@code hello} the executor opened with, once it has attached.
     *
     * @return the hello frame's header
     * @throws InterruptedException if the wait is interrupted
     */
    public JsonNode awaitHello() throws InterruptedException {
        assertTrue(connected.await(SETTLE_MS, TimeUnit.MILLISECONDS), "the executor never attached");
        JsonNode frame = hello.get();
        assertNotNull(frame, "no hello frame arrived");
        return frame;
    }

    /**
     * Write one header-only frame.
     *
     * @param header the frame's fields
     * @throws IOException if the connection is gone
     */
    public void send(Map<String, Object> header) throws IOException {
        OutputStream stream = out;
        if (stream == null) {
            return;
        }
        stream.write(JSON.writeValueAsBytes(header));
        stream.write('\n');
        stream.flush();
    }

    /**
     * Dispatch a job with the default framing.
     *
     * @param id the job's id
     * @param taskName the task to run
     * @param payload the encoded call
     * @throws IOException if the connection is gone
     */
    public void sendJob(String id, String taskName, byte[] payload) throws IOException {
        sendJob(id, taskName, payload, List.of(), null);
    }

    /**
     * Dispatch a job, resolving what only a scheduler can resolve.
     *
     * @param id the job's id
     * @param taskName the task to run
     * @param payload the encoded call
     * @param disabledMiddleware middleware the operator has toggled off
     * @param metadataJson the job's metadata, or {@code null}
     * @throws IOException if the connection is gone
     */
    public void sendJob(
            String id, String taskName, byte[] payload, List<String> disabledMiddleware, @Nullable String metadataJson)
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

    /** One already-committed step, as a dispatch's snapshot carries it. */
    public record SnapshotStep(int seq, String stepKey, String kind, byte @Nullable [] result, @Nullable Long wakeAt) {
        /**
         * A committed {@code step.run} and the bytes it memoized.
         *
         * @param seq the step's position in the job's sequence
         * @param stepKey the step's identity
         * @param result the memoized bytes
         * @return the snapshot entry
         */
        public static SnapshotStep run(int seq, String stepKey, byte[] result) {
            return new SnapshotStep(seq, stepKey, "run", result, null);
        }
    }

    /**
     * Send the steps a job has already committed, as the dispatch's own
     * {@code job_steps} frame.
     *
     * <p>Must precede {@link #sendJob} for that id: the executor decodes it on
     * arrival and keys it by job, and a snapshot that lands after the dispatch is
     * one the attempt has already replayed without. No frame at all is an empty
     * snapshot, which is why one is only sent when there is something in it.
     *
     * <p>The payload is the core's own encoding — a JSON metadata line, then
     * every step's blob concatenated in {@code seq} order. A {@code result}
     * inside the JSON would render as an array of numbers and inflate the frame
     * several-fold.
     *
     * @param jobId the job the snapshot belongs to
     * @param steps the steps already committed, in {@code seq} order
     * @throws IOException if the connection is gone
     */
    public void sendJobSteps(String jobId, List<SnapshotStep> steps) throws IOException {
        List<Map<String, Object>> metadata = new ArrayList<>();
        ByteArrayOutputStream blobs = new ByteArrayOutputStream();
        for (SnapshotStep step : steps) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("seq", step.seq());
            entry.put("step_key", step.stepKey());
            entry.put("kind", step.kind());
            entry.put("result_len", step.result() == null ? null : step.result().length);
            entry.put("wake_at", step.wakeAt());
            entry.put("created_at", 0);
            metadata.add(entry);
            if (step.result() != null) {
                blobs.write(step.result());
            }
        }
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(JSON.writeValueAsBytes(metadata));
        payload.write('\n');
        payload.write(blobs.toByteArray());

        OutputStream stream = out;
        if (stream == null) {
            return;
        }
        byte[] bytes = payload.toByteArray();
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("type", "job_steps");
        header.put("job_id", jobId);
        header.put("payload_len", bytes.length);
        stream.write(JSON.writeValueAsBytes(header));
        stream.write('\n');
        stream.write(bytes);
        stream.flush();
    }

    /**
     * Acknowledge a {@code step_commit}, which is what the task is blocked on.
     *
     * @param commit the commit frame to answer
     * @param wakeAt the deadline a sleep was actually rescheduled to, or {@code null}
     * @throws IOException if the connection is gone
     */
    public void ackStep(Frame commit, @Nullable Long wakeAt) throws IOException {
        Map<String, Object> ack = new LinkedHashMap<>();
        ack.put("type", "step_ack");
        ack.put("job_id", commit.header().path("job_id").asText());
        ack.put("seq", commit.header().path("seq").asInt());
        ack.put("ok", true);
        ack.put("already", false);
        if (wakeAt != null) {
            ack.put("wake_at", wakeAt);
        }
        send(ack);
    }

    /**
     * Refuse a {@code step_commit}, with the verdict only the storage side can make.
     *
     * @param commit the commit frame to answer
     * @param error why the write was refused
     * @param failure {@code retryable}, {@code permanent} or {@code superseded}
     * @throws IOException if the connection is gone
     */
    public void refuseStep(Frame commit, String error, String failure) throws IOException {
        Map<String, Object> ack = new LinkedHashMap<>();
        ack.put("type", "step_ack");
        ack.put("job_id", commit.header().path("job_id").asText());
        ack.put("seq", commit.header().path("seq").asInt());
        ack.put("ok", false);
        ack.put("already", false);
        ack.put("error", error);
        ack.put("failure", failure);
        send(ack);
    }

    /**
     * Every side-channel frame a job produced, plus its result.
     *
     * <p>The result is ordered behind them on one connection, so its arrival
     * is what proves the collection is complete rather than merely early.
     *
     * @return the frames, terminal one last
     * @throws IOException if the connection is gone
     */
    public List<Frame> collectUntilResult() throws IOException {
        List<Frame> collected = new ArrayList<>();
        for (; ; ) {
            Frame frame = nextFrame();
            collected.add(frame);
            if (!frame.type().equals("progress") && !frame.type().equals("task_log")) {
                return collected;
            }
        }
    }

    /**
     * The header of the next frame that is not a heartbeat.
     *
     * @return that frame's header
     * @throws IOException if the connection is gone
     */
    public JsonNode nextResult() throws IOException {
        return nextFrame().header();
    }

    /**
     * The next frame that is not a heartbeat.
     *
     * @return that frame
     * @throws IOException if the connection is gone
     */
    public Frame nextFrame() throws IOException {
        if (!pending.isEmpty()) {
            return pending.removeFirst();
        }
        Frame frame = readNonHeartbeat();
        if (frame == null) {
            throw new AssertionError("no frame arrived");
        }
        return frame;
    }

    /**
     * The next frame of a given type, buffering everything read before it.
     *
     * @param type the frame type to wait for
     * @return the first frame of that type
     * @throws IOException if the connection is gone
     */
    public Frame nextFrame(String type) throws IOException {
        for (Frame buffered : pending) {
            if (buffered.type().equals(type)) {
                pending.remove(buffered);
                return buffered;
            }
        }
        for (; ; ) {
            Frame frame = readNonHeartbeat();
            if (frame == null) {
                throw new AssertionError("no " + type + " frame arrived");
            }
            if (frame.type().equals(type)) {
                return frame;
            }
            pending.addLast(frame);
        }
    }

    /**
     * The next frame that carries meaning, or {@code null} once the wait is up.
     *
     * <p>The deadline spans the whole loop rather than each read: an attached
     * executor heartbeats every few tens of milliseconds, so a per-read timeout
     * would be reset forever by a connection that is alive but silent about the
     * frame the test is waiting for.
     */
    private @Nullable Frame readNonHeartbeat() throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SETTLE_MS);
        for (; ; ) {
            Frame frame;
            try {
                frame = readFrame();
            } catch (SocketTimeoutException expired) {
                return null;
            }
            if (frame == null) {
                return null;
            }
            if (!frame.type().equals("heartbeat")) {
                return frame;
            }
            if (System.nanoTime() > deadline) {
                return null;
            }
        }
    }

    /** Drop the connection without closing the listener. */
    public void disconnect() {
        Socket client = socket.get();
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                // Already gone, which is the state this method wanted.
            }
        }
    }

    @Override
    public void close() throws IOException {
        disconnect();
        server.close();
        accepting.interrupt();
    }
}
