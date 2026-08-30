package org.byteveda.flexiq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Coverage of the Phase B ops endpoints: settings, overrides, metrics, scaler, probes. */
@Timeout(30)
class DashboardOpsTest {

    @SuppressWarnings("unchecked")
    private static Class<Map<String, Object>> mapType() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    private static FlexiQ seededQueue(Path dir) {
        FlexiQ queue = FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
        Task<Map<String, Object>> add = Task.of("email.send", mapType());
        queue.enqueue(
                add,
                Collections.singletonMap("a", 1),
                EnqueueOptions.builder().queue("emails").build());
        return queue;
    }

    private static HttpResponse<String> probe(int port, String path) throws Exception {
        return HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void settingsCrud(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = seededQueue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            assertEquals(
                    200,
                    client.put("/api/settings/theme", "{\"value\":\"dark\"}").statusCode());
            assertTrue(client.get("/api/settings/theme").body().contains("\"value\":\"dark\""));
            assertTrue(client.get("/api/settings").body().contains("theme"));
            assertTrue(client.delete("/api/settings/theme").body().contains("\"deleted\":true"));
            assertEquals(
                    400, client.put("/api/settings/auth:x", "{\"value\":\"1\"}").statusCode());
        }
    }

    @Test
    void taskAndQueueOverrides(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = seededQueue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            assertEquals(
                    200,
                    client.put("/api/tasks/email.send/override", "{\"max_concurrent\":3}")
                            .statusCode());
            assertTrue(client.get("/api/tasks/email.send/override").body().contains("\"max_concurrent\":3"));
            assertTrue(client.get("/api/tasks").body().contains("email.send"));
            assertTrue(client.delete("/api/tasks/email.send/override").body().contains("\"cleared\":true"));

            // Queue override with paused=true pauses the queue live.
            client.put("/api/queues/emails/override", "{\"paused\":true}");
            assertTrue(client.get("/api/queues/paused").body().contains("emails"));
        }
    }

    @Test
    void aggregatedMetricsAndOps(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = seededQueue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            // No metrics recorded yet → empty aggregate object / timeseries array.
            assertEquals("{}", client.get("/api/metrics").body());
            assertEquals("[]", client.get("/api/metrics/timeseries").body());
            assertEquals("[]", client.get("/api/circuit-breakers").body());

            String eventTypes = client.get("/api/event-types").body();
            assertTrue(eventTypes.contains("job.completed") && eventTypes.contains("job.dead"));
            assertTrue(eventTypes.contains("workflow.submitted") && eventTypes.contains("predicate.rejected"));

            String scaler = client.get("/api/scaler").body();
            assertTrue(scaler.contains("\"metric_name\":\"flexiq_queue_depth\""));
            assertTrue(scaler.contains("\"metric_value\":1"));
        }
    }

    @Test
    void probesAreServed(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = seededQueue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            int port = server.port();
            assertTrue(probe(port, "/health").body().contains("\"status\":\"ok\""));
            assertTrue(probe(port, "/readiness").body().contains("\"status\":\"ready\""));
            HttpResponse<String> metrics = probe(port, "/metrics");
            assertEquals(200, metrics.statusCode());
            assertTrue(metrics.body().contains("flexiq_jobs_pending 1"));
            assertTrue(metrics.headers().firstValue("content-type").orElse("").startsWith("text/plain"));
        }
    }

    @Test
    void readinessAnswers503WhenDegraded(@TempDir Path dir) throws Exception {
        FlexiQ queue = seededQueue(dir);
        try (DashboardServer server = DashboardServer.start(queue, 0)) {
            int port = server.port();
            assertEquals(200, probe(port, "/readiness").statusCode());

            // Storage gone: the probe must fail the check, or an orchestrator
            // keeps routing to an instance that cannot serve the queue API.
            queue.close();
            HttpResponse<String> degraded = probe(port, "/readiness");
            assertEquals(503, degraded.statusCode());
            // The body still explains which dependency failed.
            assertTrue(degraded.body().contains("\"status\":\"degraded\""));
        }
    }

    @Test
    void malformedNumericParamsReturn400(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = seededQueue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            assertEquals(400, client.get("/api/metrics?since=abc").statusCode());
            assertEquals(400, client.get("/api/dead-letters?limit=xyz").statusCode());
            assertEquals(400, client.get("/api/scaler?target=nope").statusCode());
            assertEquals(400, client.get("/api/jobs?limit=notanint").statusCode());
        }
    }
}
