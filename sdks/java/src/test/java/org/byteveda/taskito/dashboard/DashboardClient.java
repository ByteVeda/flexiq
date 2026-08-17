package org.byteveda.taskito.dashboard;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.dashboard.auth.AuthStore;
import org.byteveda.taskito.dashboard.auth.Role;
import org.byteveda.taskito.dashboard.auth.Session;
import org.byteveda.taskito.dashboard.store.SettingsAccess;

/**
 * Test seam for the dashboard API: seeds users/sessions straight into the KV
 * store (bypassing login) and attaches the session cookie + CSRF header to
 * requests. Mirrors Python's {@code _testing.AuthedClient}.
 */
final class DashboardClient {
    private final HttpClient http = HttpClient.newHttpClient();
    private final String base;
    private String sessionToken;
    private String csrfToken;

    DashboardClient(int port) {
        this.base = "http://localhost:" + port;
    }

    DashboardClient as(Session session) {
        this.sessionToken = session.token();
        this.csrfToken = session.csrfToken();
        return this;
    }

    HttpResponse<String> get(String path) throws Exception {
        HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(base + path)).GET();
        applyCookies(builder);
        return send(builder);
    }

    HttpResponse<String> post(String path, String json) throws Exception {
        return body("POST", path, json, true);
    }

    HttpResponse<String> put(String path, String json) throws Exception {
        return body("PUT", path, json, true);
    }

    HttpResponse<String> delete(String path) throws Exception {
        return body("DELETE", path, null, true);
    }

    HttpResponse<String> postWithoutCsrf(String path, String json) throws Exception {
        return body("POST", path, json, false);
    }

    private HttpResponse<String> body(String method, String path, String json, boolean withCsrf) throws Exception {
        HttpRequest.BodyPublisher publisher =
                json == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json);
        HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(base + path)).method(method, publisher);
        if (json != null) {
            builder.header("Content-Type", "application/json");
        }
        applyCookies(builder);
        if (withCsrf && csrfToken != null) {
            builder.header("X-CSRF-Token", csrfToken);
        }
        return send(builder);
    }

    private void applyCookies(HttpRequest.Builder builder) {
        if (sessionToken == null) {
            return;
        }
        String cookie = "flexiq_session=" + sessionToken;
        if (csrfToken != null) {
            cookie += "; taskito_csrf=" + csrfToken;
        }
        builder.header("Cookie", cookie);
    }

    private HttpResponse<String> send(HttpRequest.Builder builder) throws Exception {
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    static AuthStore store(Taskito queue) {
        return new AuthStore(SettingsAccess.of(queue));
    }

    static Session seedAdmin(Taskito queue) {
        return seedUser(queue, "admin", Role.ADMIN);
    }

    static Session seedUser(Taskito queue, String username, Role role) {
        AuthStore store = store(queue);
        store.createUser(username, "password123", role);
        return store.createSession(username, role);
    }
}
