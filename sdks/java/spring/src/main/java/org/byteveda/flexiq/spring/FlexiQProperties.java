package org.byteveda.flexiq.spring;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for the auto-configured {@link org.byteveda.flexiq.FlexiQ} bean, bound from {@code flexiq.*}. */
@ConfigurationProperties(prefix = "flexiq")
public class FlexiQProperties {

    /** Constructs an unbound instance; Spring populates it from {@code flexiq.*}. */
    public FlexiQProperties() {}

    /** Connection URL / DSN (e.g. a SQLite path, or a {@code postgres://}/{@code redis://} URL). */
    private @Nullable String url;

    /** Connection-pool size; unset uses the backend default. */
    private @Nullable Integer poolSize;

    /** Optional namespace isolating this app's jobs within a shared store. */
    private @Nullable String namespace;

    /**
     * Returns the connection URL / DSN.
     *
     * @return the URL, or null to use the backend default
     */
    public @Nullable String getUrl() {
        return url;
    }

    /**
     * Sets the connection URL / DSN.
     *
     * @param url the URL, or null to use the backend default
     */
    public void setUrl(@Nullable String url) {
        this.url = url;
    }

    /**
     * Returns the connection-pool size.
     *
     * @return the pool size, or null to use the backend default
     */
    public @Nullable Integer getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the connection-pool size.
     *
     * @param poolSize the pool size, or null to use the backend default
     */
    public void setPoolSize(@Nullable Integer poolSize) {
        this.poolSize = poolSize;
    }

    /**
     * Returns the namespace isolating this app's jobs.
     *
     * @return the namespace, or null for an unscoped queue
     */
    public @Nullable String getNamespace() {
        return namespace;
    }

    /**
     * Sets the namespace isolating this app's jobs.
     *
     * @param namespace the namespace, or null for an unscoped queue
     */
    public void setNamespace(@Nullable String namespace) {
        this.namespace = namespace;
    }

    /** Dashboard server settings, bound from {@code flexiq.dashboard.*}. */
    private final Dashboard dashboard = new Dashboard();

    /**
     * Returns the nested dashboard settings.
     *
     * @return the dashboard settings, never null
     */
    public Dashboard getDashboard() {
        return dashboard;
    }

    /** Auto-configuration for the bundled dashboard HTTP server. */
    public static class Dashboard {

        /** Constructs an unbound instance; Spring populates it from {@code flexiq.dashboard.*}. */
        public Dashboard() {}

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

        /**
         * Returns whether a {@code DashboardServer} bean is auto-started.
         *
         * @return true if the dashboard starts with the context
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Sets whether a {@code DashboardServer} bean is auto-started.
         *
         * @param enabled true to start the dashboard with the context
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Returns the port the dashboard binds.
         *
         * @return the port, or 0 for an ephemeral one
         */
        public int getPort() {
            return port;
        }

        /**
         * Sets the port the dashboard binds.
         *
         * @param port the port, or 0 for an ephemeral one
         */
        public void setPort(int port) {
            this.port = port;
        }

        /**
         * Returns whether session authentication is enforced.
         *
         * @return true if login/setup, CSRF and RBAC are enforced
         */
        public boolean isAuthEnabled() {
            return authEnabled;
        }

        /**
         * Sets whether session authentication is enforced.
         *
         * @param authEnabled true to enforce login/setup, CSRF and RBAC
         */
        public void setAuthEnabled(boolean authEnabled) {
            this.authEnabled = authEnabled;
        }

        /**
         * Returns the shared token gating {@code /api/*}.
         *
         * @return the token, or null to use the session flow
         */
        public @Nullable String getToken() {
            return token;
        }

        /**
         * Sets the shared token gating {@code /api/*}, which overrides the session flow.
         *
         * @param token the token, or null to use the session flow
         */
        public void setToken(@Nullable String token) {
            this.token = token;
        }

        /**
         * Returns the unpacked SPA directory.
         *
         * @return the directory, or null to auto-discover the bundled assets
         */
        public @Nullable String getStaticDir() {
            return staticDir;
        }

        /**
         * Sets the unpacked SPA directory.
         *
         * @param staticDir the directory, or null to auto-discover the bundled assets
         */
        public void setStaticDir(@Nullable String staticDir) {
            this.staticDir = staticDir;
        }

        /**
         * Returns whether the {@code Secure} cookie attribute is kept.
         *
         * @return true if cookies are marked {@code Secure}
         */
        public boolean isSecureCookies() {
            return secureCookies;
        }

        /**
         * Sets whether the {@code Secure} cookie attribute is kept. Drop it for local HTTP.
         *
         * @param secureCookies true to mark cookies {@code Secure}
         */
        public void setSecureCookies(boolean secureCookies) {
            this.secureCookies = secureCookies;
        }
    }
}
