package org.byteveda.flexiq.autoscale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.scaler.Scaler;
import org.byteveda.flexiq.scaler.ScalerOptions;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class ScalerTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Task<Integer> TASK = Task.of("s.task", Integer.class);

    @Test
    @Timeout(30)
    void reportsQueueDepthAndHealth(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("s.db").toString()).open()) {
            queue.enqueue(TASK, 1);
            queue.enqueue(TASK, 2);
            try (Scaler scaler = Scaler.start(queue, new ScalerOptions(0, "127.0.0.1", 5, null))) {
                HttpClient client = HttpClient.newHttpClient();
                String base = "http://127.0.0.1:" + scaler.port();

                HttpResponse<String> scale = get(client, base + "/api/scaler");
                assertEquals(200, scale.statusCode());
                JsonNode body = JSON.readTree(scale.body());
                assertEquals(2, body.get("metricValue").asLong());
                assertEquals(5, body.get("targetValue").asInt());
                assertEquals("all", body.get("queueName").asText());

                HttpResponse<String> health = get(client, base + "/health");
                assertEquals(200, health.statusCode());
                assertEquals("ok", JSON.readTree(health.body()).get("status").asText());
            }
        }
    }

    @Test
    @Timeout(30)
    void filtersDepthByQueue(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sq.db").toString()).open()) {
            queue.enqueue(TASK, 1); // default queue
            queue.enqueue(TASK, 2, EnqueueOptions.builder().queue("high").build());
            queue.enqueue(TASK, 3, EnqueueOptions.builder().queue("high").build());
            try (Scaler scaler = Scaler.start(queue, new ScalerOptions(0, "127.0.0.1", 5, null))) {
                HttpClient client = HttpClient.newHttpClient();
                String base = "http://127.0.0.1:" + scaler.port();

                HttpResponse<String> scoped = get(client, base + "/api/scaler?queue=high");
                assertEquals(200, scoped.statusCode());
                JsonNode body = JSON.readTree(scoped.body());
                assertEquals(2, body.get("metricValue").asLong(), "only the 'high' queue counts");
                assertEquals("high", body.get("queueName").asText());
            }
        }
    }

    @Test
    @Timeout(30)
    void rejectsNonGetRequests(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sm.db").toString()).open()) {
            try (Scaler scaler = Scaler.start(queue, new ScalerOptions(0, "127.0.0.1", 5, null))) {
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest post = HttpRequest.newBuilder(
                                URI.create("http://127.0.0.1:" + scaler.port() + "/api/scaler"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();
                HttpResponse<String> response = client.send(post, HttpResponse.BodyHandlers.ofString());
                assertEquals(405, response.statusCode());
            }
        }
    }

    @Test
    void rejectsNonPositiveTarget() {
        assertThrows(IllegalArgumentException.class, () -> new ScalerOptions(0, "127.0.0.1", 0, null));
    }

    private static HttpResponse<String> get(HttpClient client, String url) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
