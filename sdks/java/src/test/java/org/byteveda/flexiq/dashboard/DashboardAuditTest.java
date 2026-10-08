package org.byteveda.flexiq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.auth.Role;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every answered dashboard request is handed to the audit trail with who made
 * it and how it ended. The record shape built from it is the core's, and is
 * pinned there; this pins what the dashboard hands over.
 */
@Timeout(30)
class DashboardAuditTest {

    /** One {@code recordDashboardAction} call. */
    record Recorded(String method, String path, int status, @Nullable String username) {}

    /** A real queue whose dashboard-audit calls are also captured. */
    private static FlexiQ capturing(FlexiQ real, List<Recorded> calls) {
        return (FlexiQ) Proxy.newProxyInstance(
                FlexiQ.class.getClassLoader(), new Class<?>[] {FlexiQ.class}, (proxy, method, args) -> {
                    if (method.getName().equals("recordDashboardAction")) {
                        calls.add(
                                new Recorded((String) args[0], (String) args[1], (Integer) args[2], (String) args[3]));
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static FlexiQ open(Path dir) {
        return FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
    }

    private static int post(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.discarding())
                .statusCode();
    }

    @Test
    void signedInChangesAreHandedOverRefusalsIncluded(@TempDir Path dir) throws Exception {
        List<Recorded> calls = new CopyOnWriteArrayList<>();
        try (FlexiQ real = open(dir)) {
            FlexiQ queue = capturing(real, calls);
            try (DashboardServer server = DashboardServer.start(queue, 0, true)) {
                int port = server.port();
                DashboardClient admin = new DashboardClient(port).as(DashboardClient.seedAdmin(queue));
                DashboardClient viewer =
                        new DashboardClient(port).as(DashboardClient.seedUser(queue, "vera", Role.VIEWER));
                assertEquals(200, admin.post("/api/queues/emails/pause", null).statusCode());
                assertEquals(200, admin.get("/api/queues/paused").statusCode());
                assertEquals(403, viewer.post("/api/queues/emails/resume", null).statusCode());
                // No session: no one to name, so nothing is handed over.
                assertEquals(401, post(port, "/api/queues/emails/resume"));
            }
        }
        assertEquals(
                List.of(
                        new Recorded("POST", "/api/queues/emails/pause", 200, "admin"),
                        new Recorded("GET", "/api/queues/paused", 200, "admin"),
                        new Recorded("POST", "/api/queues/emails/resume", 403, "vera")),
                calls);
    }

    @Test
    void anOpenDashboardHandsOverAnonymously(@TempDir Path dir) throws Exception {
        List<Recorded> calls = new CopyOnWriteArrayList<>();
        try (FlexiQ real = open(dir)) {
            FlexiQ queue = capturing(real, calls);
            try (DashboardServer server = DashboardServer.start(queue, 0)) {
                assertEquals(200, post(server.port(), "/api/queues/emails/pause"));
                // The auth routes are off with auth off; nothing changed.
                assertEquals(404, post(server.port(), "/api/auth/logout"));
            }
        }
        assertEquals(List.of(new Recorded("POST", "/api/queues/emails/pause", 200, null)), calls);
    }

    /** A dashboard that fails to bind never closes, so the writer it started must stop there. */
    @Test
    void aFailedBindStopsTheAuditWriter(@TempDir Path dir) throws Exception {
        List<String> lifecycle = new CopyOnWriteArrayList<>();
        try (FlexiQ real = open(dir);
                DashboardServer taken = DashboardServer.start(real, 0)) {
            FlexiQ queue = (FlexiQ) Proxy.newProxyInstance(
                    FlexiQ.class.getClassLoader(), new Class<?>[] {FlexiQ.class}, (proxy, method, args) -> {
                        if (method.getName().endsWith("DashboardAudit")) {
                            lifecycle.add(method.getName());
                        }
                        try {
                            return method.invoke(real, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            assertThrows(java.net.BindException.class, () -> DashboardServer.start(queue, taken.port()));
        }
        assertEquals(List.of("startDashboardAudit", "closeDashboardAudit"), lifecycle);
    }

    @Test
    void theRetentionWindowIsAtLeastADay() {
        assertEquals(90, DashboardServer.auditRetentionDays(Map.of()));
        assertEquals(7, DashboardServer.auditRetentionDays(Map.of(DashboardServer.AUDIT_RETENTION_ENV, "7")));
        for (String refused : List.of("0", "-1", "7d")) {
            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> DashboardServer.auditRetentionDays(Map.of(DashboardServer.AUDIT_RETENTION_ENV, refused)));
            assertEquals(true, error.getMessage().contains(DashboardServer.AUDIT_RETENTION_ENV));
        }
    }
}
