package org.byteveda.flexiq.dashboard.auth.oauth.config;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.byteveda.flexiq.dashboard.auth.oauth.error.OAuthConfigError;
import org.jspecify.annotations.Nullable;

/**
 * Operator-facing OAuth configuration, parsed entirely from environment
 * variables (secrets are never persisted to the settings DB).
 *
 * <p>{@code redirectBaseUrl} is the public origin the dashboard is served at;
 * every callback URL is derived from it. OAuth is considered disabled when no
 * provider is configured. {@link #fromEnv(Map)} fails fast on partial provider
 * configuration rather than silently ignoring it.
 *
 * @param redirectBaseUrl the public origin the dashboard is served at; every callback URL derives from it
 * @param google the Google provider's configuration, or {@code null} when it is not configured
 * @param github the GitHub provider's configuration, or {@code null} when it is not configured
 * @param oidc every generic OIDC provider configured, in display order
 * @param passwordAuthEnabled whether password login is offered alongside the providers
 * @param adminEmails addresses granted the admin role; every other OAuth login is a viewer
 */
public record OAuthConfig(
        String redirectBaseUrl,
        @Nullable GoogleConfig google,
        @Nullable GitHubConfig github,
        List<OidcConfig> oidc,
        boolean passwordAuthEnabled,
        List<String> adminEmails) {

    // ---- env var names (cross-SDK contract — do not rename) ----------------

    /** Absolute base URL the callback paths are appended to. */
    public static final String ENV_REDIRECT_BASE_URL = "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL";

    /** Set falsy to make the deployment OAuth-only. */
    public static final String ENV_PASSWORD_AUTH_ENABLED = "FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED";

    /** Comma-separated addresses that may bootstrap to admin. */
    public static final String ENV_ADMIN_EMAILS = "FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS";

    /** Google OAuth client id; its presence is what enables the provider. */
    public static final String ENV_GOOGLE_CLIENT_ID = "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID";

    /** Google OAuth client secret. */
    public static final String ENV_GOOGLE_CLIENT_SECRET = "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET";

    /** Comma-separated hosted domains Google logins are restricted to. */
    public static final String ENV_GOOGLE_ALLOWED_DOMAINS = "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_ALLOWED_DOMAINS";

    /** GitHub OAuth client id; its presence is what enables the provider. */
    public static final String ENV_GITHUB_CLIENT_ID = "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID";

    /** GitHub OAuth client secret. */
    public static final String ENV_GITHUB_CLIENT_SECRET = "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET";

    /** Comma-separated organisations GitHub logins are restricted to. */
    public static final String ENV_GITHUB_ALLOWED_ORGS = "FLEXIQ_DASHBOARD_OAUTH_GITHUB_ALLOWED_ORGS";

    /** Comma-separated OIDC slots to configure. */
    public static final String ENV_OIDC_PROVIDERS = "FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS";

    /** Prefix of the per-slot OIDC variables, e.g. {@code ..._OIDC_OKTA_CLIENT_ID}. */
    public static final String ENV_OIDC_PREFIX = "FLEXIQ_DASHBOARD_OAUTH_OIDC_";

    static final Pattern SLOT_PATTERN = Pattern.compile("^[a-z][a-z0-9_-]{0,31}$");
    static final Set<String> RESERVED_SLOTS = Set.of("google", "github");
    /** Hosts where {@code http://} is accepted for a redirect URL (dev only). */
    static final Set<String> LOCAL_HOSTS = Set.of("localhost", "127.0.0.1", "::1");

    /** Refuses a redirect base URL the providers could not send a browser back to. */
    public OAuthConfig {
        validateRedirectBaseUrl(redirectBaseUrl);
        oidc = List.copyOf(oidc);
        adminEmails = List.copyOf(adminEmails);
    }

    // ---- introspection -----------------------------------------------------

    /**
     * Whether any provider is configured.
     *
     * @return {@code false} when only a base URL was set
     */
    public boolean isEnabled() {
        return google != null || github != null || !oidc.isEmpty();
    }

    /**
     * Configured providers in display order (Google, GitHub, then OIDC slots).
     *
     * @return the providers, in the order the login UI shows them
     */
    public List<ProviderConfig> providers() {
        List<ProviderConfig> out = new ArrayList<>();
        if (google != null) {
            out.add(google);
        }
        if (github != null) {
            out.add(github);
        }
        out.addAll(oidc);
        return out;
    }

    /**
     * One provider by slot.
     *
     * @param slot the slot from the request path
     * @return the config, or empty when nothing is registered under it
     */
    public Optional<ProviderConfig> findProvider(String slot) {
        return providers().stream().filter(p -> p.slot().equals(slot)).findFirst();
    }

    /**
     * Compact provider summary for the login UI (no secrets), in display order.
     *
     * @return one entry per provider: slot, label and type
     */
    public List<Map<String, Object>> providersListing() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (ProviderConfig p : providers()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("slot", p.slot());
            entry.put("label", p.label());
            entry.put("type", p.type());
            out.add(entry);
        }
        return out;
    }

    /**
     * Where a provider must send the browser back.
     *
     * @param slot the provider's slot
     * @return the absolute callback URL, which must match what the provider has
     *     registered
     */
    public String callbackUrl(String slot) {
        return stripTrailingSlashes(redirectBaseUrl) + "/api/auth/oauth/callback/" + slot;
    }

    // ---- env parsing -------------------------------------------------------

    /**
     * Parse from {@link System#getenv()}.
     *
     * @return the configuration, or empty when OAuth is not configured at all
     */
    public static Optional<OAuthConfig> fromEnv() {
        return fromEnv(System.getenv());
    }

    /**
     * Parse from {@code env}. Empty when neither a base URL nor any provider is
     * configured (OAuth off). Throws {@link OAuthConfigError} on partial provider
     * configuration or an unusable redirect URL.
     *
     * @param env the variables to read, injected so tests need no process environment
     * @return the configuration, or empty when OAuth is not configured at all
     */
    public static Optional<OAuthConfig> fromEnv(Map<String, String> env) {
        String baseUrl = trimmed(env, ENV_REDIRECT_BASE_URL);
        String googleId = trimmed(env, ENV_GOOGLE_CLIENT_ID);
        String githubId = trimmed(env, ENV_GITHUB_CLIENT_ID);
        String oidcRaw = trimmed(env, ENV_OIDC_PROVIDERS);

        boolean anyProviderSignal = !googleId.isEmpty() || !githubId.isEmpty() || !oidcRaw.isEmpty();
        if (!anyProviderSignal && baseUrl.isEmpty()) {
            return Optional.empty();
        }
        if (anyProviderSignal && baseUrl.isEmpty()) {
            throw new OAuthConfigError(ENV_REDIRECT_BASE_URL + " must be set when any OAuth provider is configured");
        }

        GoogleConfig google = googleId.isEmpty() ? null : parseGoogle(env);
        GitHubConfig github = githubId.isEmpty() ? null : parseGithub(env);
        List<OidcConfig> oidc = parseOidcSlots(env, oidcRaw);
        boolean passwordEnabled = parseBool(env.getOrDefault(ENV_PASSWORD_AUTH_ENABLED, "true"), true);
        List<String> adminEmails = splitCsv(env.get(ENV_ADMIN_EMAILS));

        OAuthConfig config = new OAuthConfig(baseUrl, google, github, oidc, passwordEnabled, adminEmails);
        if (!config.isEnabled() && !passwordEnabled) {
            throw new OAuthConfigError("password auth disabled but no OAuth providers configured — no way to log in");
        }
        return Optional.of(config);
    }

    private static @Nullable GoogleConfig parseGoogle(Map<String, String> env) {
        String clientId = trimmed(env, ENV_GOOGLE_CLIENT_ID);
        String secret = trimmed(env, ENV_GOOGLE_CLIENT_SECRET);
        if (secret.isEmpty()) {
            throw new OAuthConfigError(ENV_GOOGLE_CLIENT_SECRET + " is required when the Google client id is set");
        }
        return new GoogleConfig(clientId, secret, splitCsv(env.get(ENV_GOOGLE_ALLOWED_DOMAINS)));
    }

    private static @Nullable GitHubConfig parseGithub(Map<String, String> env) {
        String clientId = trimmed(env, ENV_GITHUB_CLIENT_ID);
        String secret = trimmed(env, ENV_GITHUB_CLIENT_SECRET);
        if (secret.isEmpty()) {
            throw new OAuthConfigError(ENV_GITHUB_CLIENT_SECRET + " is required when the GitHub client id is set");
        }
        return new GitHubConfig(clientId, secret, splitCsv(env.get(ENV_GITHUB_ALLOWED_ORGS)));
    }

    private static List<OidcConfig> parseOidcSlots(Map<String, String> env, String slotsRaw) {
        List<String> slotNames = splitCsv(slotsRaw);
        if (slotNames.isEmpty()) {
            return List.of();
        }
        List<OidcConfig> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String rawSlot : slotNames) {
            String slot = rawSlot.toLowerCase(Locale.ROOT);
            if (!seen.add(slot)) {
                throw new OAuthConfigError("OIDC slot '" + slot + "' listed twice in " + ENV_OIDC_PROVIDERS);
            }
            out.add(parseOidcSlot(env, slot));
        }
        return out;
    }

    private static OidcConfig parseOidcSlot(Map<String, String> env, String slot) {
        String prefix = ENV_OIDC_PREFIX + slot.toUpperCase(Locale.ROOT).replace('-', '_');
        String clientId = trimmed(env, prefix + "_CLIENT_ID");
        String secret = trimmed(env, prefix + "_CLIENT_SECRET");
        String discovery = trimmed(env, prefix + "_DISCOVERY_URL");
        if (clientId.isEmpty() || secret.isEmpty() || discovery.isEmpty()) {
            throw new OAuthConfigError(
                    "OIDC slot '" + slot + "' requires " + prefix + "_CLIENT_ID, _CLIENT_SECRET, and _DISCOVERY_URL");
        }
        String label = trimmed(env, prefix + "_LABEL");
        if (label.isEmpty()) {
            label = titleCase(slot.replace('-', ' ').replace('_', ' '));
        }
        List<String> allowed = splitCsv(env.get(prefix + "_ALLOWED_DOMAINS"));
        // The OidcConfig constructor validates the slot pattern / reserved names.
        return new OidcConfig(slot, clientId, secret, discovery, allowed, label);
    }

    // ---- validation helpers ------------------------------------------------

    private static void validateRedirectBaseUrl(String url) {
        if (url == null || url.isEmpty()) {
            throw new OAuthConfigError("redirect base URL must be set when OAuth is enabled");
        }
        URI parsed;
        try {
            parsed = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new OAuthConfigError("redirect base URL is not a valid URL: " + url);
        }
        String scheme = parsed.getScheme() == null ? "" : parsed.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            throw new OAuthConfigError("redirect base URL must be http(s), got '" + scheme + "'");
        }
        String host = hostOf(parsed);
        if (host.isEmpty()) {
            throw new OAuthConfigError("redirect base URL must include a hostname");
        }
        if (scheme.equals("http") && !LOCAL_HOSTS.contains(host)) {
            throw new OAuthConfigError("redirect base URL must use https for non-local host " + host);
        }
    }

    /** Host with any IPv6 brackets stripped, lowercased; empty when absent. */
    private static String hostOf(URI uri) {
        String host = uri.getHost();
        if (host == null) {
            return "";
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return host.toLowerCase(Locale.ROOT);
    }

    private static String stripTrailingSlashes(String url) {
        int end = url.length();
        while (end > 0 && url.charAt(end - 1) == '/') {
            end--;
        }
        return url.substring(0, end);
    }

    private static List<String> splitCsv(@Nullable String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String part : raw.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    private static boolean parseBool(String raw, boolean fallback) {
        String lowered = raw.strip().toLowerCase(Locale.ROOT);
        return switch (lowered) {
            case "1", "true", "yes", "on" -> true;
            case "0", "false", "no", "off" -> false;
            default -> fallback;
        };
    }

    private static String titleCase(String value) {
        StringBuilder out = new StringBuilder(value.length());
        boolean startOfWord = true;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isWhitespace(c)) {
                startOfWord = true;
                out.append(c);
            } else if (startOfWord) {
                out.append(Character.toUpperCase(c));
                startOfWord = false;
            } else {
                out.append(Character.toLowerCase(c));
            }
        }
        return out.toString();
    }

    private static String trimmed(Map<String, String> env, String key) {
        String value = env.get(key);
        return value == null ? "" : value.trim();
    }

    // ---- provider sub-configs ---------------------------------------------

    /** Common shape shared by the three provider configs (no secrets exposed). */
    public sealed interface ProviderConfig permits GoogleConfig, GitHubConfig, OidcConfig {
        /**
         * URL-safe registry key used in the callback path.
         *
         * @return the slot
         */
        String slot();

        /**
         * Human-readable button label rendered by the dashboard.
         *
         * @return the label
         */
        String label();

        /**
         * One of {@code "google"}, {@code "github"}, {@code "oidc"} — picks the icon.
         *
         * @return the type
         */
        String type();
    }

    /**
     * Sign-in with Google.
     *
     * @param clientId the OAuth client id
     * @param clientSecret the OAuth client secret
     * @param allowedDomains hosted domains admitted, or empty to admit any verified
     *     Google account
     */
    public record GoogleConfig(String clientId, String clientSecret, List<String> allowedDomains)
            implements ProviderConfig {
        /** Defensively copies the allowlist. */
        public GoogleConfig {
            allowedDomains = List.copyOf(allowedDomains);
        }

        @Override
        public String slot() {
            return "google";
        }

        @Override
        public String label() {
            return "Google";
        }

        @Override
        public String type() {
            return "google";
        }
    }

    /**
     * Sign-in with GitHub.
     *
     * @param clientId the OAuth client id
     * @param clientSecret the OAuth client secret
     * @param allowedOrgs organisations admitted, or empty to admit any account;
     *     checked during the code exchange, since it needs the access token
     */
    public record GitHubConfig(String clientId, String clientSecret, List<String> allowedOrgs)
            implements ProviderConfig {
        /** Defensively copies the allowlist. */
        public GitHubConfig {
            allowedOrgs = List.copyOf(allowedOrgs);
        }

        @Override
        public String slot() {
            return "github";
        }

        @Override
        public String label() {
            return "GitHub";
        }

        @Override
        public String type() {
            return "github";
        }
    }

    /**
     * Sign-in with any standards-compliant OIDC provider.
     *
     * @param slot the URL-safe key this provider is registered under
     * @param clientId the OAuth client id
     * @param clientSecret the OAuth client secret
     * @param discoveryUrl the provider's {@code .well-known} document
     * @param allowedDomains email domains admitted, or empty to admit any
     * @param label the button label shown on the login page
     */
    public record OidcConfig(
            String slot,
            String clientId,
            String clientSecret,
            String discoveryUrl,
            List<String> allowedDomains,
            String label)
            implements ProviderConfig {
        /** Refuses a slot that is malformed or collides with a built-in provider. */
        public OidcConfig {
            if (slot == null || !SLOT_PATTERN.matcher(slot).matches()) {
                throw new OAuthConfigError("OIDC slot '" + slot + "' must match " + SLOT_PATTERN.pattern());
            }
            if (RESERVED_SLOTS.contains(slot)) {
                throw new OAuthConfigError("OIDC slot '" + slot + "' collides with a built-in provider");
            }
            if (discoveryUrl == null || discoveryUrl.isBlank()) {
                throw new OAuthConfigError("OIDC slot '" + slot + "': discovery URL is required");
            }
            allowedDomains = List.copyOf(allowedDomains);
        }

        @Override
        public String type() {
            return "oidc";
        }
    }
}
