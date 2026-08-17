package org.byteveda.taskito.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.dashboard.auth.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** End-to-end coverage of the session auth flow, RBAC, CSRF, and legacy token mode. */
@Timeout(30)
class DashboardAuthTest {

    private static Taskito open(Path dir) {
        return Taskito.builder().sqlite(dir.resolve("t.db").toString()).open();
    }

    private static HttpResponse<String> raw(int port, String method, String path, String json) throws Exception {
        HttpRequest.BodyPublisher publisher =
                json == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json);
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .method(method, publisher)
                .header("Content-Type", "application/json")
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void gatesUntilFirstAdminExists(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            int port = server.port();
            assertEquals(503, raw(port, "GET", "/api/stats", null).statusCode());
            assertTrue(raw(port, "GET", "/api/auth/status", null).body().contains("\"setup_required\":true"));
        }
    }

    @Test
    void setupCreatesAdminOnce(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            int port = server.port();
            HttpResponse<String> setup =
                    raw(port, "POST", "/api/auth/setup", "{\"username\":\"root\",\"password\":\"password123\"}");
            assertEquals(200, setup.statusCode());
            assertTrue(setup.body().contains("\"role\":\"admin\""));
            // Second setup rejected.
            assertEquals(
                    400,
                    raw(port, "POST", "/api/auth/setup", "{\"username\":\"x\",\"password\":\"password123\"}")
                            .statusCode());
            // With a user present, status flips.
            assertTrue(raw(port, "GET", "/api/auth/status", null).body().contains("\"setup_required\":false"));
        }
    }

    @Test
    void loginSetsCookiesAndRedactsToken(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            int port = server.port();
            raw(port, "POST", "/api/auth/setup", "{\"username\":\"root\",\"password\":\"password123\"}");
            HttpResponse<String> login =
                    raw(port, "POST", "/api/auth/login", "{\"username\":\"root\",\"password\":\"password123\"}");
            assertEquals(200, login.statusCode());
            List<String> cookies = login.headers().allValues("set-cookie");
            assertTrue(cookies.stream().anyMatch(c -> c.startsWith("flexiq_session=") && c.contains("HttpOnly")));
            assertTrue(cookies.stream().anyMatch(c -> c.startsWith("taskito_csrf=") && !c.contains("HttpOnly")));
            // Raw session token never leaks into the JSON body.
            assertFalse(login.body().contains("\"token\""));
            assertTrue(login.body().contains("\"csrf_token\""));
        }
    }

    @Test
    void rejectsBadCredentials(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            int port = server.port();
            raw(port, "POST", "/api/auth/setup", "{\"username\":\"root\",\"password\":\"password123\"}");
            HttpResponse<String> bad =
                    raw(port, "POST", "/api/auth/login", "{\"username\":\"root\",\"password\":\"nope\"}");
            assertEquals(400, bad.statusCode());
            assertTrue(bad.body().contains("invalid_credentials"));
        }
    }

    @Test
    void unauthenticatedApiIsRejected(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            DashboardClient.seedAdmin(queue); // a user now exists
            assertEquals(401, raw(server.port(), "GET", "/api/stats", null).statusCode());
        }
    }

    @Test
    void authenticatedReadsSucceed(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            assertEquals(200, client.get("/api/stats").statusCode());
            assertEquals(200, client.get("/api/auth/whoami").statusCode());
        }
    }

    @Test
    void csrfIsRequiredForWrites(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            assertEquals(
                    403,
                    client.postWithoutCsrf("/api/queues/emails/pause", null).statusCode());
            assertEquals(200, client.post("/api/queues/emails/pause", null).statusCode());
        }
    }

    @Test
    void viewersCannotWrite(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            DashboardClient.seedAdmin(queue); // keep setup satisfied
            DashboardClient viewer =
                    new DashboardClient(server.port()).as(DashboardClient.seedUser(queue, "read-only", Role.VIEWER));
            assertEquals(200, viewer.get("/api/stats").statusCode());
            assertEquals(403, viewer.post("/api/queues/emails/pause", null).statusCode());
        }
    }

    @Test
    void logoutInvalidatesSession(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            assertEquals(200, client.post("/api/auth/logout", null).statusCode());
            assertEquals(401, client.get("/api/auth/whoami").statusCode());
        }
    }

    @Test
    void changePasswordRotatesCredential(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            int port = server.port();
            DashboardClient client = new DashboardClient(port).as(DashboardClient.seedUser(queue, "root", Role.ADMIN));
            HttpResponse<String> changed = client.post(
                    "/api/auth/change-password", "{\"old_password\":\"password123\",\"new_password\":\"brand-new-1\"}");
            assertEquals(200, changed.statusCode());
            assertEquals(
                    200,
                    raw(port, "POST", "/api/auth/login", "{\"username\":\"root\",\"password\":\"brand-new-1\"}")
                            .statusCode());
        }
    }

    @Test
    void oauthEndpointsReport404WhenUnconfigured(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            int port = server.port();
            assertTrue(raw(port, "GET", "/api/auth/providers", null).body().contains("\"password_enabled\":true"));
            assertEquals(
                    404, raw(port, "GET", "/api/auth/oauth/start/google", null).statusCode());
        }
    }

    @Test
    void legacyTokenModeStillWorks(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, "sekret", null)) {
            int port = server.port();
            assertTrue(raw(port, "GET", "/api/auth/status", null).body().contains("\"setup_required\":false"));
            assertEquals(401, raw(port, "GET", "/api/stats", null).statusCode());
            assertEquals(200, rawWithToken(port, "/api/stats", "sekret").statusCode());
        }
    }

    @Test
    void queryTokenOnlyBootstrapsPageLoads(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, "sekret", null)) {
            int port = server.port();
            // A query token no longer authenticates API calls (it leaks into logs).
            assertEquals(401, raw(port, "GET", "/api/stats?token=sekret", null).statusCode());

            // On a page load it sets the cookie and redirects with the token stripped.
            HttpResponse<String> boot = raw(port, "GET", "/?token=sekret&tab=jobs", null);
            assertEquals(302, boot.statusCode());
            assertEquals("/?tab=jobs", boot.headers().firstValue("Location").orElse(""));
            assertTrue(boot.headers().allValues("set-cookie").stream().anyMatch(c -> c.startsWith("taskito_token=")));

            // A wrong query token neither bootstraps nor redirects.
            HttpResponse<String> miss = raw(port, "GET", "/?token=nope", null);
            assertTrue(miss.statusCode() != 302);
            assertTrue(miss.headers().allValues("set-cookie").isEmpty());
        }
    }

    @Test
    void probesRequireSessionWhenAuthEnabled(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, true)) {
            int port = server.port();
            assertEquals(200, raw(port, "GET", "/health", null).statusCode());
            assertEquals(401, raw(port, "GET", "/readiness", null).statusCode());
            assertEquals(401, raw(port, "GET", "/metrics", null).statusCode());

            DashboardClient client = new DashboardClient(port).as(DashboardClient.seedAdmin(queue));
            assertEquals(200, client.get("/readiness").statusCode());
            assertEquals(200, client.get("/metrics").statusCode());
        }
    }

    @Test
    void probesRequireTokenInLegacyMode(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0, "sekret", null)) {
            int port = server.port();
            assertEquals(200, raw(port, "GET", "/health", null).statusCode());
            assertEquals(401, raw(port, "GET", "/readiness", null).statusCode());
            assertEquals(200, rawWithToken(port, "/readiness", "sekret").statusCode());
        }
    }

    private static HttpResponse<String> rawWithToken(int port, String path, String token) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("X-FlexiQ-Token", token)
                .GET()
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void openModeIsTheDefault(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            int port = server.port();
            HttpResponse<String> status = raw(port, "GET", "/api/auth/status", null);
            assertEquals(200, status.statusCode());
            assertTrue(status.body().contains("\"auth_enabled\":false"));
            assertTrue(status.body().contains("\"setup_required\":false"));
            assertEquals(200, raw(port, "GET", "/api/stats", null).statusCode());
        }
    }

    @Test
    void openModeAllowsWritesWithoutCsrf(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            assertEquals(
                    200,
                    raw(server.port(), "POST", "/api/queues/emails/pause", null).statusCode());
        }
    }

    @Test
    void openModeStaysOpenWhenUsersExist(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient.seedAdmin(queue);
            assertEquals(200, raw(server.port(), "GET", "/api/stats", null).statusCode());
        }
    }

    @Test
    void openModeRejectsAuthEndpoints(@TempDir Path dir) throws Exception {
        try (Taskito queue = open(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            int port = server.port();
            for (String path : List.of("/api/auth/whoami", "/api/auth/providers")) {
                HttpResponse<String> response = raw(port, "GET", path, null);
                assertEquals(404, response.statusCode(), path);
                assertTrue(response.body().contains("auth_disabled"), path);
            }
            for (String path : List.of("/api/auth/login", "/api/auth/setup", "/api/auth/logout")) {
                HttpResponse<String> response = raw(port, "POST", path, "{}");
                assertEquals(404, response.statusCode(), path);
                assertTrue(response.body().contains("auth_disabled"), path);
            }
        }
    }
}
