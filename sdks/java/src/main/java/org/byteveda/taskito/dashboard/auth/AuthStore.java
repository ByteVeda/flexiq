package org.byteveda.taskito.dashboard.auth;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.byteveda.taskito.dashboard.store.SettingsAccess;
import org.byteveda.taskito.dashboard.support.DashboardError;
import org.byteveda.taskito.dashboard.support.Json;
import org.jspecify.annotations.Nullable;

/**
 * Users and sessions persisted in the settings KV store — no dedicated tables,
 * so the design is backend-agnostic and shared with the other SDKs.
 *
 * <ul>
 *   <li>{@code auth:users} — a single JSON object {@code {username: row}}.
 *   <li>{@code auth:session:<token>} — one JSON row per session; timestamps in
 *       <b>seconds</b> (user timestamps are milliseconds).
 * </ul>
 *
 * Passwords use {@link PasswordHasher} (PBKDF2 600k); unknown-user logins pay a
 * dummy verification so timing can't enumerate accounts.
 */
public final class AuthStore {
    public static final String USERS_KEY = "auth:users";
    public static final String SESSION_PREFIX = "auth:session:";
    public static final long DEFAULT_SESSION_TTL_SECONDS = 24 * 60 * 60;

    public static final String ENV_ADMIN_USER = "TASKITO_DASHBOARD_ADMIN_USER";
    public static final String ENV_ADMIN_PASSWORD = "TASKITO_DASHBOARD_ADMIN_PASSWORD";

    private static final int USERNAME_MAX_LEN = 64;
    private static final int PASSWORD_MIN_LEN = 8;
    private static final int PASSWORD_MAX_LEN = 256;

    private final SettingsAccess settings;

    public AuthStore(SettingsAccess settings) {
        this.settings = settings;
    }

    // ---- users -------------------------------------------------------------

    public int countUsers() {
        return rawUsers().size();
    }

    public Optional<User> getUser(String username) {
        return userRow(rawUsers(), username).map(row -> toUser(username, row));
    }

    /** One user's record out of the shared blob, when it is present and well-formed. */
    private static Optional<Map<String, Object>> userRow(Map<String, Object> users, String username) {
        Object row = users.get(username);
        if (!(row instanceof Map<?, ?> map)) {
            return Optional.empty();
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> typed = (Map<String, Object>) map;
        return Optional.of(typed);
    }

    /**
     * Create a password user (default role {@code admin}). Mutations of the
     * shared {@code auth:users} blob are {@code synchronized} so concurrent
     * requests in this process can't lose an update; the KV store has no
     * cross-process CAS, so a second writer in another process is still a
     * theoretical race (bounded to first-run setup, which is single-shot).
     */
    public synchronized User createUser(String username, String password, String role) {
        return createUser(username, password, parseRole(role));
    }

    /** Create a password user with a typed role. */
    public synchronized User createUser(String username, String password, Role role) {
        validateUsername(username);
        validatePassword(password);
        if (role == null) {
            throw DashboardError.badRequest("invalid role");
        }
        if (rawUsers().containsKey(username)) {
            throw DashboardError.badRequest("user already exists");
        }
        String hash = PasswordHasher.hash(password);
        Map<String, Object> users = rawUsers();
        if (users.containsKey(username)) {
            throw DashboardError.badRequest("user already exists");
        }
        long now = nowMillis();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("password_hash", hash);
        row.put("role", role.wire());
        row.put("created_at", now);
        row.put("last_login_at", null);
        users.put(username, row);
        saveUsers(users);
        return new User(username, hash, role.wire(), now, null, null, null);
    }

    /** Verify credentials; {@code null} on any failure. Updates last-login on success. */
    public @Nullable User authenticate(String username, String password) {
        Optional<User> found = getUser(username);
        if (found.isEmpty()) {
            PasswordHasher.dummyVerify(password);
            return null;
        }
        User user = found.get();
        if (!PasswordHasher.verify(password, user.passwordHash())) {
            return null;
        }
        return touchLastLogin(user);
    }

    /** Verify a password without side effects (used by change-password). */
    public boolean verifyPassword(User user, String password) {
        return PasswordHasher.verify(password, user.passwordHash());
    }

    /** Change a user's password and revoke their existing sessions. */
    public synchronized void updatePassword(String username, String newPassword) {
        validatePassword(newPassword);
        Map<String, Object> users = rawUsers();
        Object rowObj = users.get(username);
        if (!(rowObj instanceof Map)) {
            throw DashboardError.badRequest("not_authenticated");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rowObj;
        row.put("password_hash", PasswordHasher.hash(newPassword));
        saveUsers(users);
        // A password change must not leave stolen/older sessions valid.
        deleteSessionsForUser(username);
    }

    public synchronized void deleteUser(String username) {
        Map<String, Object> users = rawUsers();
        if (users.remove(username) != null) {
            saveUsers(users);
        }
        deleteSessionsForUser(username);
    }

    private synchronized User touchLastLogin(User user) {
        long now = nowMillis();
        Map<String, Object> users = rawUsers();
        Object rowObj = users.get(user.username());
        if (rowObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rowObj;
            row.put("last_login_at", now);
            saveUsers(users);
        }
        return new User(
                user.username(),
                user.passwordHash(),
                user.role(),
                user.createdAt(),
                now,
                user.email(),
                user.displayName());
    }

    // ---- OAuth users (used by the OAuth flow) ------------------------------

    /**
     * Fetch-or-provision the user behind an OAuth identity. Username is
     * {@code <slot>:<subject>} (never the email). New users get a role from
     * {@link #oauthBootstrapRole}; existing users keep their role but refresh
     * email/display-name and last-login.
     */
    public synchronized User getOrCreateOauthUser(
            String slot,
            String subject,
            @Nullable String email,
            @Nullable String name,
            boolean emailVerified,
            List<String> adminEmails) {
        String username = slot + ":" + subject;
        Map<String, Object> users = rawUsers();
        Optional<Map<String, Object>> existing = userRow(users, username);
        if (existing.isPresent()) {
            Map<String, Object> row = existing.get();
            row.put("email", email);
            row.put("display_name", name);
            row.put("last_login_at", nowMillis());
            saveUsers(users);
            return toUser(username, row);
        }
        Role role = oauthBootstrapRole(email, emailVerified, adminEmails);
        long now = nowMillis();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("password_hash", PasswordHasher.oauthSentinel(slot));
        row.put("role", role.wire());
        row.put("created_at", now);
        row.put("last_login_at", now);
        row.put("email", email);
        row.put("display_name", name);
        users.put(username, row);
        saveUsers(users);
        return toUser(username, row);
    }

    /**
     * Role for a freshly seen OAuth user: admin requires a verified email AND a
     * listed address. Everyone else — including the very first user — gets
     * viewer, so a stray first OAuth login can never win admin.
     */
    public static Role oauthBootstrapRole(@Nullable String email, boolean emailVerified, List<String> adminEmails) {
        if (!emailVerified || email == null || email.isBlank()) {
            return Role.VIEWER;
        }
        String normalised = email.toLowerCase(Locale.ROOT);
        boolean listed = adminEmails != null
                && adminEmails.stream().anyMatch(e -> e.toLowerCase(Locale.ROOT).equals(normalised));
        return listed ? Role.ADMIN : Role.VIEWER;
    }

    // ---- sessions ----------------------------------------------------------

    public Session createSession(String username, Role role) {
        return createSession(username, role, DEFAULT_SESSION_TTL_SECONDS);
    }

    /** Open a session from a wire-form role ({@code "admin"} / {@code "viewer"}). */
    public Session createSession(String username, String role) {
        return createSession(username, parseRole(role), DEFAULT_SESSION_TTL_SECONDS);
    }

    /** Open a session from a wire-form role, with an explicit TTL. */
    public Session createSession(String username, String role, long ttlSeconds) {
        return createSession(username, parseRole(role), ttlSeconds);
    }

    public Session createSession(String username, Role role, long ttlSeconds) {
        if (role == null) {
            throw DashboardError.badRequest("invalid role");
        }
        String token = Tokens.session();
        String csrf = Tokens.session();
        long now = nowSeconds();
        long expires = now + ttlSeconds;
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("username", username);
        row.put("role", role.wire());
        row.put("created_at", now);
        row.put("expires_at", expires);
        row.put("csrf_token", csrf);
        settings.setSetting(SESSION_PREFIX + token, Json.toString(row));
        return new Session(token, username, role.wire(), now, expires, csrf);
    }

    /** Resolve a session token; deletes and returns empty if expired/malformed. */
    public Optional<Session> getSession(String token) {
        if (token == null || token.isEmpty()) {
            return Optional.empty();
        }
        Optional<String> raw = settings.getSetting(SESSION_PREFIX + token);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> data = Json.parseMap(raw.get());
        if (data == null) {
            return Optional.empty();
        }
        Session session;
        try {
            session = new Session(
                    token,
                    Json.requireString(data, "username"),
                    Role.orViewer(Json.optionalString(data, "role")).wire(),
                    Json.requireLong(data, "created_at"),
                    Json.requireLong(data, "expires_at"),
                    Json.requireString(data, "csrf_token"));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (session.isExpired(nowSeconds())) {
            deleteSession(token);
            return Optional.empty();
        }
        return Optional.of(session);
    }

    public void deleteSession(String token) {
        settings.deleteSetting(SESSION_PREFIX + token);
    }

    public int pruneExpiredSessions() {
        long now = nowSeconds();
        int removed = 0;
        for (Map.Entry<String, String> entry : settings.listSettings().entrySet()) {
            if (!entry.getKey().startsWith(SESSION_PREFIX)) {
                continue;
            }
            Map<String, Object> data = Json.parseMap(entry.getValue());
            if (data == null) {
                continue;
            }
            long expires;
            try {
                expires = Json.requireLong(data, "expires_at");
            } catch (RuntimeException e) {
                continue;
            }
            if (expires <= now) {
                settings.deleteSetting(entry.getKey());
                removed++;
            }
        }
        return removed;
    }

    private void deleteSessionsForUser(String username) {
        for (Map.Entry<String, String> entry : settings.listSettings().entrySet()) {
            if (!entry.getKey().startsWith(SESSION_PREFIX)) {
                continue;
            }
            Map<String, Object> data = Json.parseMap(entry.getValue());
            if (data != null && username.equals(data.get("username"))) {
                settings.deleteSetting(entry.getKey());
            }
        }
    }

    // ---- env bootstrap -----------------------------------------------------

    /**
     * Seed an admin from {@code TASKITO_DASHBOARD_ADMIN_USER}/{@code _PASSWORD}
     * if set and the user does not yet exist. Idempotent; safe every startup.
     *
     * <p>Unlike Python/Node, the JVM cannot scrub the password out of its own
     * environment (it is read-only), so the variable remains visible to the
     * process — prefer first-run setup where that matters.
     */
    public void bootstrapAdminFromEnv() {
        bootstrapAdminFromEnv(System.getenv());
    }

    void bootstrapAdminFromEnv(Map<String, String> env) {
        String username = env.get(ENV_ADMIN_USER);
        String password = env.get(ENV_ADMIN_PASSWORD);
        if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
            return;
        }
        if (getUser(username).isPresent()) {
            return;
        }
        createUser(username, password, Role.ADMIN);
    }

    // ---- helpers -----------------------------------------------------------

    private Map<String, Object> rawUsers() {
        Optional<String> raw = settings.getSetting(USERS_KEY);
        if (raw.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> parsed = Json.parseMap(raw.get());
        return parsed != null ? parsed : new LinkedHashMap<>();
    }

    private void saveUsers(Map<String, Object> users) {
        settings.setSetting(USERS_KEY, Json.toString(users));
    }

    private static User toUser(String username, Map<String, Object> row) {
        return new User(
                username,
                Json.requireString(row, "password_hash"),
                Role.orViewer(Json.optionalString(row, "role")).wire(),
                Json.requireLong(row, "created_at"),
                Json.optionalLong(row, "last_login_at"),
                Json.optionalString(row, "email"),
                Json.optionalString(row, "display_name"));
    }

    private static long nowMillis() {
        return System.currentTimeMillis();
    }

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    private static void validateUsername(String username) {
        if (username == null || username.isEmpty() || username.length() > USERNAME_MAX_LEN) {
            throw DashboardError.badRequest("username must be 1-64 characters");
        }
        for (int i = 0; i < username.length(); i++) {
            char c = username.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '.' && c != '_' && c != '-') {
                throw DashboardError.badRequest("username may only contain letters, digits, '.', '_', '-'");
            }
        }
    }

    /** Strictly parse a wire-form role; anything outside the set is a request error. */
    private static Role parseRole(String role) {
        Role parsed = Role.fromWire(role);
        if (parsed == null) {
            throw DashboardError.badRequest("invalid role");
        }
        return parsed;
    }

    private static void validatePassword(String password) {
        if (password == null || password.length() < PASSWORD_MIN_LEN || password.length() > PASSWORD_MAX_LEN) {
            throw DashboardError.badRequest("password must be 8-256 characters");
        }
    }
}
