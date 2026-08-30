package org.byteveda.flexiq.dashboard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.api.CoreHandlers;
import org.byteveda.flexiq.dashboard.api.MetricsHandlers;
import org.byteveda.flexiq.dashboard.api.OpsHandlers;
import org.byteveda.flexiq.dashboard.api.OverridesHandlers;
import org.byteveda.flexiq.dashboard.api.RetentionHandlers;
import org.byteveda.flexiq.dashboard.api.SettingsHandlers;
import org.byteveda.flexiq.dashboard.api.WebhooksHandlers;
import org.byteveda.flexiq.dashboard.api.WorkflowsHandlers;
import org.byteveda.flexiq.dashboard.auth.AuthHandlers;
import org.byteveda.flexiq.dashboard.auth.AuthStore;
import org.byteveda.flexiq.dashboard.auth.Cookies;
import org.byteveda.flexiq.dashboard.auth.Policy;
import org.byteveda.flexiq.dashboard.auth.RequestContext;
import org.byteveda.flexiq.dashboard.auth.TokenAuth;
import org.byteveda.flexiq.dashboard.auth.oauth.OAuthFlow;
import org.byteveda.flexiq.dashboard.auth.oauth.OAuthHandlers;
import org.byteveda.flexiq.dashboard.auth.oauth.OAuthStateStore;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.error.OAuthConfigError;
import org.byteveda.flexiq.dashboard.auth.oauth.provider.OAuthProvider;
import org.byteveda.flexiq.dashboard.routing.Req;
import org.byteveda.flexiq.dashboard.routing.Router;
import org.byteveda.flexiq.dashboard.store.OverridesStore;
import org.byteveda.flexiq.dashboard.store.SettingsAccess;
import org.byteveda.flexiq.dashboard.support.DashboardError;
import org.byteveda.flexiq.dashboard.support.Http;
import org.byteveda.flexiq.health.ReadinessReport;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.jspecify.annotations.Nullable;

/**
 * A read/action dashboard API + static-SPA server backed by a {@link FlexiQ},
 * built on the JDK's {@code com.sun.net.httpserver}. The JSON contract is
 * snake_case with Unix-millisecond timestamps.
 *
 * <p>Three auth modes:
 * <ul>
 *   <li><b>Open</b> (default): no authentication — every route serves openly
 *       and the auth endpoints (except {@code /api/auth/status}) respond 404.
 *   <li><b>Session</b> ({@code authEnabled=true}): password users + sessions in
 *       the settings KV, CSRF double-submit, admin/viewer RBAC, first-run
 *       setup. Bootstrap an admin with
 *       {@code FLEXIQ_DASHBOARD_ADMIN_USER}/{@code _PASSWORD}.
 *   <li><b>Legacy token</b>: pass a shared {@code token} to gate {@code /api/*}
 *       as a fixed admin identity (no users/sessions). Kept for back-compat.
 * </ul>
 */
public final class DashboardServer implements AutoCloseable {
    private static final FlexiQLogger LOG = FlexiQLogger.create("dashboard");
    private static final String METRICS_TOKEN_ENV = "FLEXIQ_DASHBOARD_METRICS_TOKEN";

    /**
     * Defense-in-depth headers on every response. The CSP assumes a fully
     * self-contained SPA bundle (no inline scripts — the theme bootstrap ships
     * as /theme-init.js); inline style attributes need 'unsafe-inline' in
     * style-src.
     */
    private static final String[][] SECURITY_HEADERS = {
        {
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
                    + "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; "
                    + "base-uri 'self'; form-action 'self'; object-src 'none'"
        },
        {"X-Content-Type-Options", "nosniff"},
        {"X-Frame-Options", "DENY"},
        {"Referrer-Policy", "same-origin"},
    };

    private final HttpServer server;
    private final FlexiQ queue;
    private final @Nullable Path staticDir;
    private final boolean secureCookies;

    private final AuthStore authStore;
    private final @Nullable TokenAuth tokenAuth;
    private final boolean authEnabled;
    private final AuthHandlers authHandlers;
    private final OAuthHandlers oauthHandlers;
    private final OpsHandlers ops;
    private final String metricsToken;
    private final Router router;

    private DashboardServer(
            HttpServer server,
            FlexiQ queue,
            @Nullable Path staticDir,
            boolean secureCookies,
            @Nullable String token,
            boolean authEnabled,
            @Nullable OAuthFlow oauth) {
        this.server = server;
        this.queue = queue;
        this.staticDir = staticDir;
        this.secureCookies = secureCookies;
        this.authStore = new AuthStore(SettingsAccess.of(queue));
        this.tokenAuth = token != null ? new TokenAuth(token) : null;
        this.authEnabled = authEnabled;
        this.authHandlers = new AuthHandlers(authStore);
        this.oauthHandlers = new OAuthHandlers(oauth, secureCookies);
        this.ops = new OpsHandlers(queue);
        this.metricsToken = System.getenv(METRICS_TOKEN_ENV);
        this.router = buildRouter();
    }

    /**
     * Start on {@code port} (0 = ephemeral) in open mode — no authentication.
     *
     * @param queue the queue the dashboard reads and acts on
     * @param port the port to bind, or 0 for an ephemeral one
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    public static DashboardServer start(FlexiQ queue, int port) throws IOException {
        return start(queue, port, null, null, true, false);
    }

    /**
     * Start on {@code port}; {@code authEnabled=true} enables the session-auth flow.
     *
     * @param queue the queue the dashboard reads and acts on
     * @param port the port to bind, or 0 for an ephemeral one
     * @param authEnabled {@code true} for password users, sessions and RBAC;
     *     {@code false} serves every route openly
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    public static DashboardServer start(FlexiQ queue, int port, boolean authEnabled) throws IOException {
        return start(queue, port, null, null, true, authEnabled);
    }

    /**
     * Start in legacy shared-token mode; the session flow is disabled.
     *
     * @param queue the queue the dashboard reads and acts on
     * @param port the port to bind, or 0 for an ephemeral one
     * @param token the shared bearer token every request must carry, or {@code null}
     *     to serve openly
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    public static DashboardServer start(FlexiQ queue, int port, @Nullable String token) throws IOException {
        return start(queue, port, token, null, true, false);
    }

    /**
     * As {@link #start(FlexiQ, int, String)} but with an unpacked SPA directory.
     *
     * @param queue the queue the dashboard reads and acts on
     * @param port the port to bind, or 0 for an ephemeral one
     * @param token the shared bearer token, or {@code null} to serve openly
     * @param staticDir where the built SPA lives, or {@code null} to auto-discover
     *     the bundled one
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    public static DashboardServer start(FlexiQ queue, int port, @Nullable String token, @Nullable String staticDir)
            throws IOException {
        return start(queue, port, token, staticDir, true, false);
    }

    /**
     * As the full variant with auth disabled (the default).
     *
     * @param queue the queue the dashboard reads and acts on
     * @param port the port to bind, or 0 for an ephemeral one
     * @param token the shared bearer token, or {@code null} to serve openly
     * @param staticDir where the built SPA lives, or {@code null} to auto-discover
     *     the bundled one
     * @param secureCookies {@code false} drops the {@code Secure} cookie attribute,
     *     for local HTTP development
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    public static DashboardServer start(
            FlexiQ queue, int port, @Nullable String token, @Nullable String staticDir, boolean secureCookies)
            throws IOException {
        return start(queue, port, token, staticDir, secureCookies, false);
    }

    /**
     * Start on {@code port} (0 = ephemeral). A non-null {@code token} selects the
     * legacy shared-token mode; otherwise {@code authEnabled} picks the session
     * flow (true) or open mode (false, the default). A null {@code staticDir}
     * auto-discovers the bundled SPA. {@code secureCookies=false} drops the
     * {@code Secure} cookie attribute for local HTTP development.
     *
     * @param queue the queue the dashboard reads and acts on
     * @param port the port to bind, or 0 for an ephemeral one
     * @param token the shared bearer token, which selects legacy mode and wins over
     *     {@code authEnabled}; {@code null} to leave the mode to {@code authEnabled}
     * @param staticDir where the built SPA lives, or {@code null} to auto-discover
     *     the bundled one
     * @param secureCookies {@code false} drops the {@code Secure} cookie attribute,
     *     for local HTTP development
     * @param authEnabled {@code true} for the session flow, which is also the only
     *     mode OAuth is wired into
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    public static DashboardServer start(
            FlexiQ queue,
            int port,
            @Nullable String token,
            @Nullable String staticDir,
            boolean secureCookies,
            boolean authEnabled)
            throws IOException {
        // OAuth is session-mode only; open and legacy token modes have no login UI.
        boolean sessionMode = token == null && authEnabled;
        @Nullable OAuthFlow oauth = sessionMode ? buildOAuthFlow(queue, System.getenv()) : null;
        return startInternal(queue, port, token, staticDir, secureCookies, authEnabled, oauth);
    }

    /** Test seam: start in session mode with an explicitly built OAuth flow. */
    static DashboardServer startWithOAuth(FlexiQ queue, int port, boolean secureCookies, @Nullable OAuthFlow oauth)
            throws IOException {
        return startInternal(queue, port, null, null, secureCookies, true, oauth);
    }

    private static DashboardServer startInternal(
            FlexiQ queue,
            int port,
            @Nullable String token,
            @Nullable String staticDir,
            boolean secureCookies,
            boolean authEnabled,
            @Nullable OAuthFlow oauth)
            throws IOException {
        // Resolve assets before binding so a discovery failure can't leak a bound port.
        Path dir = staticDir != null ? Paths.get(staticDir).normalize() : DashboardAssets.resolveOrNull();
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        DashboardServer dashboard = new DashboardServer(server, queue, dir, secureCookies, token, authEnabled, oauth);
        // Seed an env admin before serving so no request races the open setup endpoint.
        if (token == null && authEnabled) {
            dashboard.authStore.bootstrapAdminFromEnv();
        }
        server.createContext("/", dashboard::dispatch);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        return dashboard;
    }

    /**
     * Build the OAuth flow from env, or return {@code null} to run password-only.
     *
     * <p>Degrades gracefully: an invalid config logs a warning and disables OAuth
     * rather than failing startup; a missing nimbus-jose-jwt jar (needed only by
     * the OIDC providers) is caught as a {@link LinkageError} and likewise
     * disables OAuth, leaving password login intact.
     */
    private static @Nullable OAuthFlow buildOAuthFlow(FlexiQ queue, Map<String, String> env) {
        Optional<OAuthConfig> configured;
        try {
            configured = OAuthConfig.fromEnv(env);
        } catch (OAuthConfigError e) {
            LOG.warn("Dashboard OAuth disabled: " + e.getMessage());
            return null;
        }
        if (configured.isEmpty()) {
            return null;
        }
        OAuthConfig config = configured.get();
        try {
            SettingsAccess settings = SettingsAccess.of(queue);
            Map<String, OAuthProvider> providers = OAuthFlow.buildProviders(config, HttpClient.newHttpClient());
            return new OAuthFlow(new AuthStore(settings), config, new OAuthStateStore(settings), providers);
        } catch (LinkageError e) {
            // NoClassDefFoundError (a LinkageError) when nimbus-jose-jwt is absent.
            LOG.warn("Dashboard OAuth disabled: OIDC login requires the nimbus-jose-jwt jar on the classpath");
            return null;
        }
    }

    /**
     * The bound port (useful when {@code port} was 0).
     *
     * @return the port the server actually listens on
     */
    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    // ---- dispatch ----------------------------------------------------------

    private void dispatch(HttpExchange exchange) throws IOException {
        try {
            // Single choke point: every response — JSON, assets, probes,
            // redirects — carries the security headers.
            for (String[] header : SECURITY_HEADERS) {
                exchange.getResponseHeaders().set(header[0], header[1]);
            }
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/health")) {
                Http.respondJson(exchange, 200, Map.of("status", "ok"));
            } else if (path.equals("/readiness")) {
                serveReadiness(exchange);
            } else if (path.equals("/metrics")) {
                serveMetrics(exchange);
            } else if (path.startsWith("/api/")) {
                handleApi(exchange, path);
            } else if (!tokenBootstrapRedirect(exchange, path)) {
                serveStatic(exchange, path);
            }
        } catch (DashboardError e) {
            safeRespond(exchange, e.status(), Http.errorBody(e.code()));
        } catch (RuntimeException | IOException e) {
            // Log the detail server-side; return a generic code so an unauthenticated
            // caller can't harvest internal exception text (e.g. from a malformed cookie).
            LOG.warn("dashboard request failed: " + exchange.getRequestMethod() + " " + exchange.getRequestURI(), e);
            safeRespond(exchange, 500, Http.errorBody("internal_error"));
        } finally {
            exchange.close();
        }
    }

    private void handleApi(HttpExchange exchange, String path) throws IOException {
        Map<String, String> query = Http.query(exchange);
        String method = exchange.getRequestMethod();
        if (tokenAuth != null) {
            handleTokenMode(tokenAuth, exchange, path, method, query);
            return;
        }
        if (!authEnabled) {
            handleOpenMode(exchange, path, method, query);
            return;
        }
        // OAuth routes emit redirects (not JSON), so they bypass the router; they
        // are public, so this runs before the auth gate.
        if (oauthHandlers.serve(exchange, path, method, query)) {
            return;
        }
        RequestContext ctx = RequestContext.build(exchange, authStore);
        Policy.authorize(path, method, ctx, authStore);
        if (!router.dispatch(exchange, method, path, query, ctx)) {
            Http.respondError(exchange, 404, "not found");
        }
    }

    /**
     * Open mode (auth disabled, the default): every route serves without a
     * session; the auth endpoints respond 404 so the SPA hides login affordances.
     */
    private void handleOpenMode(HttpExchange exchange, String path, String method, Map<String, String> query)
            throws IOException {
        if (path.equals("/api/auth/status")) {
            Http.respondJson(exchange, 200, Map.of("auth_enabled", false, "setup_required", false));
            return;
        }
        if (path.startsWith("/api/auth/")) {
            Http.respondError(exchange, 404, "auth_disabled");
            return;
        }
        if (!router.dispatch(exchange, method, path, query, RequestContext.open())) {
            Http.respondError(exchange, 404, "not found");
        }
    }

    private void handleTokenMode(
            TokenAuth tokenAuth, HttpExchange exchange, String path, String method, Map<String, String> query)
            throws IOException {
        if (path.equals("/api/auth/status")) {
            Http.respondJson(exchange, 200, TokenAuth.openStatus());
            return;
        }
        if (!tokenAuth.matches(tokenAuth.presented(exchange))) {
            Http.respondError(exchange, 401, "unauthorized");
            return;
        }
        if (path.equals("/api/auth/whoami")) {
            long ttl = AuthStore.DEFAULT_SESSION_TTL_SECONDS;
            exchange.getResponseHeaders().add("Set-Cookie", Cookies.sessionCookie("open", secureCookies, ttl));
            exchange.getResponseHeaders().add("Set-Cookie", Cookies.csrfCookie("open", secureCookies, ttl));
            Http.respondJson(exchange, 200, TokenAuth.openWhoami());
            return;
        }
        if (path.startsWith("/api/auth/")) {
            Http.respondError(exchange, 404, "not found");
            return;
        }
        if (!router.dispatch(exchange, method, path, query, RequestContext.open())) {
            Http.respondError(exchange, 404, "not found");
        }
    }

    // ---- routes ------------------------------------------------------------

    private Router buildRouter() {
        CoreHandlers core = new CoreHandlers(queue);
        SettingsAccess settings = SettingsAccess.of(queue);
        SettingsHandlers settingsApi = new SettingsHandlers(settings);
        RetentionHandlers retention = new RetentionHandlers(queue);
        OverridesHandlers overrides = new OverridesHandlers(queue, new OverridesStore(settings));
        MetricsHandlers metrics = new MetricsHandlers(queue);
        WorkflowsHandlers workflows = new WorkflowsHandlers(queue);
        WebhooksHandlers webhooks = new WebhooksHandlers(queue);
        long ttl = AuthStore.DEFAULT_SESSION_TTL_SECONDS;
        Router r = new Router();

        // Auth
        r.get("/api/auth/status", req -> authHandlers.status());
        r.post("/api/auth/setup", req -> authHandlers.setup(req.jsonBody()));
        r.post("/api/auth/login", req -> login(req, ttl));
        r.post("/api/auth/logout", this::logout);
        r.get("/api/auth/whoami", req -> authHandlers.whoami(req.ctx()));
        r.post("/api/auth/change-password", req -> authHandlers.changePassword(req.ctx(), req.jsonBody()));
        // /api/auth/providers and /api/auth/oauth/* are served by OAuthHandlers
        // (they emit JSON or 302 redirects) before the router runs.

        // Read
        r.get("/api/stats", req -> core.stats());
        r.get("/api/stats/queues", req -> core.statsByQueue());
        r.get("/api/queues/paused", req -> core.queuesPaused());
        r.get("/api/jobs", req -> core.listJobs(req.query()));
        r.get("/api/jobs/([^/]+)/logs", req -> core.jobLogs(req.param(0)));
        r.get("/api/jobs/([^/]+)/replay-history", req -> core.replayHistory(req.param(0)));
        r.get("/api/jobs/([^/]+)/dag", req -> core.jobDag(req.param(0)));
        r.get("/api/jobs/([^/]+)", req -> core.job(req.param(0)));
        r.get("/api/dead-letters", req -> core.listDead(req.query()));
        r.get("/api/logs", req -> core.logs(req.query()));
        r.get("/api/workers", req -> core.listWorkers());

        // Metrics (aggregated — the SPA contract, not raw rows)
        r.get("/api/metrics", req -> metrics.aggregated(req.query()));
        r.get("/api/metrics/timeseries", req -> metrics.timeseries(req.query()));

        // Ops
        r.get("/api/circuit-breakers", req -> ops.circuitBreakers());
        r.get("/api/event-types", req -> ops.eventTypes());
        r.get("/api/scaler", req -> ops.scaler(req.query()));
        r.get("/api/resources", req -> ops.resources());

        // Workflows (read)
        r.get("/api/workflows/runs", req -> workflows.runs(req.query()));
        r.get("/api/workflows/runs/([^/]+)/dag", req -> workflows.dag(req.param(0)));
        r.get("/api/workflows/runs/([^/]+)/children", req -> workflows.children(req.param(0)));
        r.get("/api/workflows/runs/([^/]+)", req -> workflows.run(req.param(0)));

        // Settings KV
        r.get("/api/retention/dry-run", req -> retention.retentionDryRun());
        r.get("/api/retention", req -> retention.retention());
        r.get("/api/settings", req -> settingsApi.list());
        r.get("/api/settings/(.+)", req -> settingsApi.get(req.param(0)));
        r.put("/api/settings/(.+)", req -> settingsApi.put(req.param(0), req.jsonBody()));
        r.delete("/api/settings/(.+)", req -> settingsApi.delete(req.param(0)));

        // Tasks / queues + overrides
        r.get("/api/tasks", req -> overrides.listTasks());
        r.get("/api/queues", req -> overrides.listQueues());
        r.get("/api/tasks/([^/]+)/override", req -> overrides.getTaskOverride(req.param(0)));
        r.put("/api/tasks/([^/]+)/override", req -> overrides.putTaskOverride(req.param(0), req.jsonBody()));
        r.delete("/api/tasks/([^/]+)/override", req -> overrides.deleteTaskOverride(req.param(0)));
        r.get("/api/queues/([^/]+)/override", req -> overrides.getQueueOverride(req.param(0)));
        r.put("/api/queues/([^/]+)/override", req -> overrides.putQueueOverride(req.param(0), req.jsonBody()));
        r.delete("/api/queues/([^/]+)/override", req -> overrides.deleteQueueOverride(req.param(0)));

        // Webhooks (CRUD + test/replay + delivery history). Specific paths first
        // so the single-segment catch-alls don't shadow the sub-resources.
        r.get("/api/webhooks", req -> webhooks.list());
        r.post("/api/webhooks", req -> webhooks.create(req.jsonBody()));
        r.post("/api/webhooks/([^/]+)/test", req -> webhooks.test(req.param(0)));
        r.post("/api/webhooks/([^/]+)/rotate-secret", req -> webhooks.rotateSecret(req.param(0)));
        r.get("/api/webhooks/([^/]+)/deliveries", req -> webhooks.deliveries(req.param(0), req.query()));
        r.get("/api/webhooks/([^/]+)/deliveries/([^/]+)", req -> webhooks.delivery(req.param(0), req.param(1)));
        r.post(
                "/api/webhooks/([^/]+)/deliveries/([^/]+)/replay",
                req -> webhooks.replayDelivery(req.param(0), req.param(1)));
        r.get("/api/webhooks/([^/]+)", req -> webhooks.get(req.param(0)));
        r.put("/api/webhooks/([^/]+)", req -> webhooks.update(req.param(0), req.jsonBody()));
        r.delete("/api/webhooks/([^/]+)", req -> webhooks.delete(req.param(0)));

        // Action
        r.post("/api/jobs/([^/]+)/replay", req -> core.replayJob(req.param(0)));
        r.post("/api/jobs/([^/]+)/cancel", req -> core.cancel(req.param(0)));
        r.post("/api/dead-letters/([^/]+)/retry", req -> core.retryDead(req.param(0)));
        r.post("/api/queues/([^/]+)/pause", req -> core.pause(req.param(0)));
        r.post("/api/queues/([^/]+)/resume", req -> core.resume(req.param(0)));

        return r;
    }

    private Object login(Req req, long ttl) {
        Map<String, Object> out = authHandlers.login(req.jsonBody());
        @SuppressWarnings("unchecked")
        Map<String, Object> session = Objects.requireNonNull(
                (Map<String, Object>) out.get("session"), "login response is missing its session");
        String token = Objects.requireNonNull((String) session.remove("token"), "session is missing its token");
        String csrf = Objects.requireNonNull((String) session.get("csrf_token"), "session is missing its CSRF token");
        var headers = req.exchange().getResponseHeaders();
        headers.add("Set-Cookie", Cookies.sessionCookie(token, secureCookies, ttl));
        headers.add("Set-Cookie", Cookies.csrfCookie(csrf, secureCookies, ttl));
        return out;
    }

    private Object logout(Req req) {
        Map<String, Object> out = authHandlers.logout(req.ctx());
        var headers = req.exchange().getResponseHeaders();
        headers.add("Set-Cookie", Cookies.clearSession(secureCookies));
        headers.add("Set-Cookie", Cookies.clearCsrf(secureCookies));
        return out;
    }

    // ---- probes ------------------------------------------------------------

    private void serveReadiness(HttpExchange exchange) throws IOException {
        if (!probeAuthorized(exchange)) {
            Http.respondError(exchange, 401, "unauthorized");
            return;
        }
        ReadinessReport report = ops.readinessReport();
        // Non-2xx on degraded so orchestrators stop routing to this instance —
        // the body still explains which dependency failed.
        Http.respondJson(exchange, report.ready() ? 200 : 503, report.toMap());
    }

    private void serveMetrics(HttpExchange exchange) throws IOException {
        if (!probeAuthorized(exchange)) {
            Http.respondError(exchange, 401, "unauthorized");
            return;
        }
        byte[] out = ops.prometheus().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, out.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(out);
        }
    }

    /**
     * Gate for {@code /readiness} and {@code /metrics}. Accepted credentials: the
     * optional {@code FLEXIQ_DASHBOARD_METRICS_TOKEN} bearer (scraper-friendly),
     * the legacy shared token in token mode, or a valid session in session mode.
     * Open mode with no metrics token stays public (probe-friendly default).
     */
    private boolean probeAuthorized(HttpExchange exchange) {
        boolean metricsTokenSet = metricsToken != null && !metricsToken.isEmpty();
        if (metricsTokenSet) {
            if (metricsBearerMatches(exchange)) {
                return true;
            }
        } else if (tokenAuth == null && !authEnabled) {
            return true;
        }
        if (tokenAuth != null) {
            return tokenAuth.matches(tokenAuth.presented(exchange));
        }
        if (authEnabled) {
            return RequestContext.build(exchange, authStore).authenticated();
        }
        return false;
    }

    private boolean metricsBearerMatches(HttpExchange exchange) {
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            return false;
        }
        String presented = authorization.substring("Bearer ".length()).trim();
        return MessageDigest.isEqual(
                metricsToken.getBytes(StandardCharsets.UTF_8), presented.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Token mode: a valid {@code ?token=} on a page load (never an API call)
     * bootstraps the httpOnly cookie, then redirects to strip the token from the
     * URL so it can't leak via history or the Referer header.
     */
    private boolean tokenBootstrapRedirect(HttpExchange exchange, String path) throws IOException {
        if (tokenAuth == null || !"GET".equals(exchange.getRequestMethod())) {
            return false;
        }
        String queryToken = Http.query(exchange).get("token");
        if (queryToken == null || !tokenAuth.matches(queryToken)) {
            return false;
        }
        exchange.getResponseHeaders().add("Set-Cookie", TokenAuth.openCookie(queryToken, secureCookies));
        exchange.getResponseHeaders().set("Location", path + queryWithoutToken(exchange));
        exchange.sendResponseHeaders(302, -1);
        return true;
    }

    /** The raw query minus the {@code token} parameter, with a leading '?' when non-empty. */
    private static String queryWithoutToken(HttpExchange exchange) {
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return "";
        }
        StringBuilder kept = new StringBuilder();
        for (String pair : raw.split("&")) {
            if (pair.equals("token") || pair.startsWith("token=")) {
                continue;
            }
            if (kept.length() > 0) {
                kept.append('&');
            }
            kept.append(pair);
        }
        return kept.length() == 0 ? "" : "?" + kept;
    }

    // ---- static + helpers --------------------------------------------------

    private void serveStatic(HttpExchange exchange, String path) throws IOException {
        if (staticDir == null) {
            Http.respondError(exchange, 404, "not found");
            return;
        }
        Path target = staticDir.resolve(path.substring(1)).normalize();
        if (!target.startsWith(staticDir) || !Files.isRegularFile(target)) {
            target = staticDir.resolve("index.html");
        }
        if (!Files.isRegularFile(target)) {
            Http.respondError(exchange, 404, "not found");
            return;
        }
        byte[] body = Files.readAllBytes(target);
        exchange.getResponseHeaders().set("Content-Type", contentType(target));
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static void safeRespond(HttpExchange exchange, int status, Object body) {
        try {
            Http.respondJson(exchange, status, body);
        } catch (IOException | RuntimeException ignored) {
            // Response already partially written or the client disconnected.
        }
    }

    private static String contentType(Path file) {
        String name = file.getFileName().toString();
        if (name.endsWith(".html")) {
            return "text/html; charset=utf-8";
        }
        if (name.endsWith(".js")) {
            return "application/javascript";
        }
        if (name.endsWith(".css")) {
            return "text/css";
        }
        if (name.endsWith(".json")) {
            return "application/json";
        }
        if (name.endsWith(".svg")) {
            return "image/svg+xml";
        }
        return "application/octet-stream";
    }
}
