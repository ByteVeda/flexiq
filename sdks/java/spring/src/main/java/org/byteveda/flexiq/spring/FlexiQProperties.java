package org.byteveda.flexiq.spring;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for the auto-configured {@link org.byteveda.flexiq.FlexiQ} bean, bound from {@code flexiq.*}. */
@ConfigurationProperties(prefix = "flexiq")
public class FlexiQProperties {
    /** Connection URL / DSN (e.g. a SQLite path, or a {@code postgres://}/{@code redis://} URL). */
    private @Nullable String url;

    /** Connection-pool size; unset uses the backend default. */
    private @Nullable Integer poolSize;

    /** Optional namespace isolating this app's jobs within a shared store. */
    private @Nullable String namespace;

    public @Nullable String getUrl() {
        return url;
    }

    public void setUrl(@Nullable String url) {
        this.url = url;
    }

    public @Nullable Integer getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(@Nullable Integer poolSize) {
        this.poolSize = poolSize;
    }

    public @Nullable String getNamespace() {
        return namespace;
    }

    public void setNamespace(@Nullable String namespace) {
        this.namespace = namespace;
    }

    /** Dashboard server settings, bound from {@code flexiq.dashboard.*}. */
    private final Dashboard dashboard = new Dashboard();

    public Dashboard getDashboard() {
        return dashboard;
    }

    /** Auto-configuration for the bundled dashboard HTTP server. */
    public static class Dashboard {
        /** Whether to auto-start a {@code DashboardServer} bean. Off by default. */
        private boolean enabled = false;

        /**
         * Port to bind (0 = ephemeral). Defaults to 8081, not 8080, so it doesn't
         * clash with Spring Boot's own embedded server; override as needed.
         */
        private int port = 8081;

        /** Whether to enforce session authentication (login/setup, CSRF, RBAC). Off by default. */
        private boolean authEnabled = false;

        /** Optional shared token gating {@code /api/*}; overrides the session flow. */
        private @Nullable String token;

        /** Optional unpacked SPA directory; null auto-discovers the bundled assets. */
        private @Nullable String staticDir;

        /** Whether to keep the {@code Secure} cookie attribute (drop it for local HTTP). */
        private boolean secureCookies = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public boolean isAuthEnabled() {
            return authEnabled;
        }

        public void setAuthEnabled(boolean authEnabled) {
            this.authEnabled = authEnabled;
        }

        public @Nullable String getToken() {
            return token;
        }

        public void setToken(@Nullable String token) {
            this.token = token;
        }

        public @Nullable String getStaticDir() {
            return staticDir;
        }

        public void setStaticDir(@Nullable String staticDir) {
            this.staticDir = staticDir;
        }

        public boolean isSecureCookies() {
            return secureCookies;
        }

        public void setSecureCookies(boolean secureCookies) {
            this.secureCookies = secureCookies;
        }
    }
}
