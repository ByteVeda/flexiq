package org.byteveda.taskito.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.task.EnqueueOptions;
import org.byteveda.taskito.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class DashboardTest {

    @SuppressWarnings("unchecked")
    private static Class<Map<String, Object>> mapType() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> getWithToken(int port, String path, String token) throws Exception {
        return HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                                .header("X-FlexiQ-Token", token)
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @Timeout(30)
    void servesSnakeCaseContract(@TempDir Path dir) throws Exception {
        Task<Map<String, Object>> add = Task.of("add", mapType());
        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(
                    add,
                    Collections.singletonMap("a", 1),
                    EnqueueOptions.builder().queue("emails").build());
            try (DashboardServer server = DashboardServer.start(queue, 0)) {
                DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));

                HttpResponse<String> stats = client.get("/api/stats");
                assertEquals(200, stats.statusCode());
                assertTrue(stats.body().contains("\"pending\":1"));

                HttpResponse<String> jobs = client.get("/api/jobs?queue=emails");
                assertTrue(jobs.body().contains("\"task_name\":\"add\""));
                assertTrue(jobs.body().contains("\"status\":\"pending\""));
            }
        }
    }

    @Test
    @Timeout(30)
    void servesSpaWithoutAStaticPath(@TempDir Path dir) throws Exception {
        // No path passed to start(); the server auto-discovers the SPA. Point the
        // discovery override at a stand-in tree to exercise that wiring offline.
        Path spa = Files.createDirectories(dir.resolve("spa"));
        Files.writeString(spa.resolve("index.html"), "<h1>taskito</h1>");
        System.setProperty("taskito.dashboard.dir", spa.toString());
        try (Taskito queue =
                        Taskito.builder().sqlite(dir.resolve("t.db").toString()).open();
                DashboardServer server = DashboardServer.start(queue, 0)) {
            int port = server.port();
            HttpResponse<String> root = get(port, "/");
            assertEquals(200, root.statusCode());
            assertTrue(root.body().contains("taskito"));
            // SPA fallback: an unknown client route still resolves to index.html.
            assertEquals(200, get(port, "/jobs").statusCode());
        } finally {
            System.clearProperty("taskito.dashboard.dir");
        }
    }

    @Test
    @Timeout(30)
    void setsSecurityHeadersOnEveryResponse(@TempDir Path dir) throws Exception {
        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            try (DashboardServer server = DashboardServer.start(queue, 0)) {
                for (String path : new String[] {"/health", "/api/stats"}) {
                    HttpResponse<String> res = get(server.port(), path);
                    assertEquals(
                            "nosniff",
                            res.headers().firstValue("X-Content-Type-Options").orElse(""),
                            path);
                    assertEquals(
                            "DENY", res.headers().firstValue("X-Frame-Options").orElse(""), path);
                    assertEquals(
                            "same-origin",
                            res.headers().firstValue("Referrer-Policy").orElse(""),
                            path);
                    assertTrue(
                            res.headers()
                                    .firstValue("Content-Security-Policy")
                                    .orElse("")
                                    .contains("default-src 'self'"),
                            path);
                }
            }
        }
    }

    @Test
    @Timeout(30)
    void enforcesToken(@TempDir Path dir) throws Exception {
        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            try (DashboardServer server = DashboardServer.start(queue, 0, "sekret", null)) {
                int port = server.port();
                assertEquals(401, get(port, "/api/stats").statusCode());
                assertEquals(401, get(port, "/api/stats?token=sekret").statusCode());
                assertEquals(200, getWithToken(port, "/api/stats", "sekret").statusCode());
            }
        }
    }

    @Test
    @Timeout(30)
    void convenienceMethodStartsServer(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                        Taskito.builder().sqlite(dir.resolve("t.db").toString()).open();
                DashboardServer server = queue.dashboard(0, "tok")) {
            assertTrue(server.port() > 0);
            assertEquals(200, getWithToken(server.port(), "/api/stats", "tok").statusCode());
        }
    }
}
