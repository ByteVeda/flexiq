package org.byteveda.flexiq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** End-to-end coverage of the webhook dashboard surface: CRUD, rotate, deliveries, and secret safety. */
@Timeout(30)
class DashboardWebhookTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String body) throws Exception {
        return JSON.readValue(body, Map.class);
    }

    @Test
    void webhookLifecycleNeverLeaksSecrets(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                        FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));

            // Create — the response reveals the secret exactly once and masks header values.
            String createBody = "{\"url\":\"https://example.com/hook\",\"events\":[\"success\"],"
                    + "\"generate_secret\":true,\"headers\":{\"X-Api-Key\":\"topsecret\"}}";
            HttpResponse<String> created = client.post("/api/webhooks", createBody);
            assertEquals(200, created.statusCode());
            Map<String, Object> createdJson = parse(created.body());
            String id = (String) createdJson.get("id");
            assertNotNull(id);
            assertEquals(Boolean.TRUE, createdJson.get("has_secret"));
            String secret = (String) createdJson.get("secret");
            assertNotNull(secret);
            assertFalse(secret.isEmpty());
            assertEquals("***", ((Map<?, ?>) createdJson.get("headers")).get("X-Api-Key"));

            // List — the hook is present, the raw secret and header value NEVER leak.
            HttpResponse<String> list = client.get("/api/webhooks");
            assertEquals(200, list.statusCode());
            assertTrue(list.body().contains(id));
            assertTrue(list.body().contains("\"has_secret\":true"));
            assertFalse(list.body().contains("\"secret\""));
            assertFalse(list.body().contains("topsecret"));
            assertFalse(list.body().contains(secret));

            // Get — same masking; the header value is ***.
            HttpResponse<String> got = client.get("/api/webhooks/" + id);
            assertEquals(200, got.statusCode());
            Map<String, Object> gotJson = parse(got.body());
            assertNull(gotJson.get("secret"));
            assertEquals("***", ((Map<?, ?>) gotJson.get("headers")).get("X-Api-Key"));

            // Update — disable it.
            HttpResponse<String> updated = client.put("/api/webhooks/" + id, "{\"enabled\":false}");
            assertEquals(200, updated.statusCode());
            assertEquals(Boolean.FALSE, parse(updated.body()).get("enabled"));

            // Rotate — a fresh secret is returned once, and it differs from the original.
            HttpResponse<String> rotated = client.post("/api/webhooks/" + id + "/rotate-secret", "{}");
            assertEquals(200, rotated.statusCode());
            String rotatedSecret = (String) parse(rotated.body()).get("secret");
            assertNotNull(rotatedSecret);
            assertNotEquals(secret, rotatedSecret);

            // Deliveries — an empty log is a 200 array.
            HttpResponse<String> deliveries = client.get("/api/webhooks/" + id + "/deliveries");
            assertEquals(200, deliveries.statusCode());
            assertEquals("[]", deliveries.body());

            // Delete — then the hook is gone.
            HttpResponse<String> deleted = client.delete("/api/webhooks/" + id);
            assertEquals(200, deleted.statusCode());
            assertTrue(deleted.body().contains("\"deleted\":true"));
            assertEquals(404, client.get("/api/webhooks/" + id).statusCode());
        }
    }
}
