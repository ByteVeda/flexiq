package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** A worker's event sinks: config errors at start, and CloudEvents reaching an HTTP receiver. */
class EventSinksTest {

    private static final Task<String> ECHO = Task.of("events.echo", String.class);
    private static final String SINK = "receiver";
    private static final String STARTED = "org.byteveda.flexiq.job.started";
    private static final String COMPLETED = "org.byteveda.flexiq.job.completed";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ConcurrentLinkedQueue<JsonNode> received = new ConcurrentLinkedQueue<>();
    /** How long the receiver holds each reply, to keep events in flight. */
    private volatile long replyDelayMillis;

    private HttpServer receiver;
    private String sinkUrl;

    @BeforeEach
    void startReceiver() throws IOException {
        receiver = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        receiver.createContext("/events", this::receive);
        receiver.start();
        sinkUrl = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/events";
    }

    @AfterEach
    void stopReceiver() {
        receiver.stop(0);
    }

    private void receive(HttpExchange exchange) throws IOException {
        try (exchange;
                InputStream body = exchange.getRequestBody()) {
            JsonNode parsed = JSON.readTree(body);
            // A batching sink posts an array; one event per request otherwise.
            if (parsed.isArray()) {
                parsed.forEach(received::add);
            } else {
                received.add(parsed);
            }
            Thread.sleep(replyDelayMillis);
            exchange.sendResponseHeaders(200, -1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String httpSinks() {
        return "{\"sinks\":[{\"kind\":\"http\",\"name\":\"" + SINK + "\",\"url\":\"" + sinkUrl
                + "\",\"allow\":[\"127.0.0.1\"],\"allow_loopback\":true}]}";
    }

    @Test
    @Timeout(60)
    void aDocumentNamingNoSinksFailsTheStart(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            Worker.Builder builder =
                    queue.worker().handle(ECHO, payload -> payload).eventSinks("{\"sinks\":[]}");
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, builder::start);
            assertTrue(error.getMessage().contains("names no sinks"), error.getMessage());
            // Refused before the worker registered anything.
            assertTrue(queue.listWorkers().isEmpty());
        }
    }

    @Test
    @Timeout(60)
    void malformedJsonFailsTheStart(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            Worker.Builder builder =
                    queue.worker().handle(ECHO, payload -> payload).eventSinks("{not json");
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, builder::start);
            assertTrue(error.getMessage().contains("events config is not valid"), error.getMessage());
        }
    }

    @Test
    @Timeout(60)
    void anHttpSinkWithAnEmptyAllowListFailsTheStart(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            String config = "{\"sinks\":[{\"kind\":\"http\",\"name\":\"" + SINK + "\",\"url\":\"" + sinkUrl
                    + "\",\"allow\":[],\"allow_loopback\":true}]}";
            Worker.Builder builder =
                    queue.worker().handle(ECHO, payload -> payload).eventSinks(config);
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, builder::start);
            assertTrue(
                    error.getMessage().contains("events sink 'receiver': allow must name at least one host"),
                    error.getMessage());
        }
    }

    @Test
    void aNegativeDrainIsRefused(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            Worker.Builder builder = queue.worker();
            assertThrows(IllegalArgumentException.class, () -> builder.eventSinksDrain(Duration.ofMillis(-1)));
        }
    }

    @Test
    @Timeout(60)
    void aWorkerWithoutSinksReportsNoStats(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            Worker worker = queue.worker().handle(ECHO, payload -> payload).start();
            try (worker) {
                assertEquals(List.of(), worker.eventSinkStats());
            }
            assertEquals(List.of(), worker.eventSinkStats());
        }
    }

    @Test
    @Timeout(60)
    void sendsJobStartedAndCompletedToAnHttpSink(@TempDir Path dir) throws Exception {
        Path config = dir.resolve("sinks.json");
        Files.writeString(config, httpSinks());
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            Worker worker = queue.worker()
                    .handle(ECHO, payload -> payload)
                    .eventSinks(config)
                    .start();
            try (worker) {
                String id = queue.enqueue(ECHO, "hello");
                queue.awaitJob(id, Duration.ofSeconds(30));

                assertTrue(
                        waitFor(() -> eventsFor(id).containsKey(STARTED)
                                && eventsFor(id).containsKey(COMPLETED)),
                        "the receiver never got both events: " + received);
                for (String type : List.of(STARTED, COMPLETED)) {
                    JsonNode event = eventsFor(id).get(type);
                    assertEquals("default", event.path("flexiqqueue").asText());
                    assertEquals(ECHO.name(), event.path("flexiqtask").asText());
                    assertEquals(id, event.path("data").path("job_id").asText());
                    // No sink set `include_payload`, so no payload leaves the process.
                    assertFalse(event.path("data").has("payload_base64"));
                }

                // The counter moves once the receiver's reply is read, just after the post.
                assertTrue(waitFor(() -> worker.eventSinkStats().get(0).delivered() >= 2));
                EventSinkStats stats = worker.eventSinkStats().get(0);
                assertEquals(SINK, stats.name());
                assertEquals("http", stats.kind());
                assertEquals(0, stats.droppedRejected());
                assertEquals(0, stats.droppedFailed());
            }
        }
    }

    @Test
    @Timeout(60)
    void closeReturnsOnlyAfterASlowSinkHasTheJobsEvents(@TempDir Path dir) throws Exception {
        replyDelayMillis = 300;
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            Worker worker = queue.worker()
                    .handle(ECHO, payload -> payload)
                    .eventSinks(httpSinks())
                    .eventSinksDrain(Duration.ofSeconds(10))
                    .start();
            String id = queue.enqueue(ECHO, "hello");
            queue.awaitJob(id, Duration.ofSeconds(30));
            // Each reply is held, so the events are still in flight when close starts.
            worker.close();

            EventSinkStats stats = worker.eventSinkStats().get(0);
            assertEquals(2, stats.delivered());
            assertEquals(0, stats.queued());
            assertEquals(0, stats.droppedShutdown());
        }
    }

    /**
     * The drain budget starts after close() has waited for in-flight handlers, so
     * a handler that outlives the budget measured from the stop still has its
     * outcome delivered.
     */
    @Test
    @Timeout(60)
    void aHandlerFinishingDuringCloseStillHasItsEventsDelivered(@TempDir Path dir) throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("e.db").toString()).open()) {
            Worker worker = queue.worker()
                    .handle(ECHO, payload -> {
                        entered.countDown();
                        Thread.sleep(500);
                        return payload;
                    })
                    .eventSinks(httpSinks())
                    .eventSinksDrain(Duration.ofMillis(200))
                    .start();
            String id = queue.enqueue(ECHO, "late");
            assertTrue(entered.await(20, TimeUnit.SECONDS), "the handler never ran");
            worker.close();

            assertTrue(eventsFor(id).containsKey(COMPLETED), "job.completed was dropped: " + received);
            EventSinkStats stats = worker.eventSinkStats().get(0);
            assertEquals(2, stats.delivered());
            assertEquals(0, stats.droppedShutdown());
        }
    }

    private Map<String, JsonNode> eventsFor(String jobId) {
        return received.stream()
                .filter(event -> jobId.equals(event.path("subject").asText()))
                .collect(Collectors.toMap(event -> event.path("type").asText(), event -> event, (a, b) -> a));
    }

    /** Poll an aggregate until it holds or 20 s pass. */
    private static boolean waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(25);
        }
        return condition.getAsBoolean();
    }
}
