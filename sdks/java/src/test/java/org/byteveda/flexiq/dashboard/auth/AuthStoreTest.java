package org.byteveda.flexiq.dashboard.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.support.DashboardError;
import org.junit.jupiter.api.Test;

class AuthStoreTest {

    private AuthStore store() {
        return new AuthStore(new InMemorySettings());
    }

    @Test
    void modelsExposeTheWireFormSoExistingCallersStillCompile() {
        // A Java enum is not a String, so typing these accessors would break every
        // `String role = session.role()` caller. The enum owns validation and the
        // decisions; the models carry the wire form the other SDKs also expose.
        AuthStore store = store();
        User user = store.createUser("alice", "password123", Role.ADMIN);
        String userRole = user.role();
        String sessionRole = store.createSession("alice", Role.ADMIN).role();
        assertEquals("admin", userRole);
        assertEquals("admin", sessionRole);
        assertEquals(Role.ADMIN, Role.fromWire(userRole));
    }

    @Test
    void storedRoleNothingRecognizesReadsBackAsViewer() {
        InMemorySettings settings = new InMemorySettings();
        AuthStore store = new AuthStore(settings);
        store.createUser("alice", "password123", Role.ADMIN);
        String users = settings.getSetting(AuthStore.USERS_KEY).orElseThrow();
        settings.setSetting(AuthStore.USERS_KEY, users.replace("\"admin\"", "\"superuser\""));

        // Fails closed on read rather than handing back an unusable role.
        assertEquals(Role.VIEWER.wire(), store.getUser("alice").orElseThrow().role());
    }

    @Test
    void createsAndAuthenticatesUsers() {
        AuthStore store = store();
        assertEquals(0, store.countUsers());
        User user = store.createUser("alice", "password123", Role.ADMIN);
        assertEquals("alice", user.username());
        assertEquals(Role.ADMIN.wire(), user.role());
        assertNull(user.lastLoginAt());
        assertEquals(1, store.countUsers());

        assertNotNull(store.authenticate("alice", "password123"));
        assertNull(store.authenticate("alice", "wrong"));
        assertNull(store.authenticate("ghost", "password123"));

        // last-login stamped on success (milliseconds).
        User reloaded = store.getUser("alice").orElseThrow();
        assertNotNull(reloaded.lastLoginAt());
        assertTrue(reloaded.lastLoginAt() > 1_000_000_000_000L);
    }

    @Test
    void rejectsDuplicatesAndInvalidInput() {
        AuthStore store = store();
        store.createUser("bob", "password123", Role.ADMIN);
        assertThrows(DashboardError.class, () -> store.createUser("bob", "password123", Role.ADMIN));
        assertThrows(DashboardError.class, () -> store.createUser("bad name", "password123", Role.ADMIN));
        assertThrows(DashboardError.class, () -> store.createUser("carol", "short", Role.ADMIN));
        assertThrows(DashboardError.class, () -> store.createUser("carol", "password123", (Role) null));
        assertThrows(DashboardError.class, () -> store.createSession("carol", (Role) null));
        // An unknown role is no longer representable at the call site; a stored one
        // still has to fail closed.
        assertEquals(Role.VIEWER, Role.orViewer("superuser"));
        // The wire-form overloads reject an unknown role rather than coercing it.
        assertThrows(DashboardError.class, () -> store.createUser("carol", "password123", "root"));
        assertThrows(DashboardError.class, () -> store.createSession("carol", "root"));
    }

    @Test
    void sessionsUseSecondsAndExpire() {
        AuthStore store = store();
        Session session = store.createSession("alice", Role.ADMIN);
        assertEquals(AuthStore.DEFAULT_SESSION_TTL_SECONDS, session.expiresAt() - session.createdAt());
        assertTrue(session.createdAt() < 1_000_000_000_000L); // seconds, not millis
        assertTrue(store.getSession(session.token()).isPresent());

        Session expired = store.createSession("alice", Role.ADMIN, -10);
        assertTrue(store.getSession(expired.token()).isEmpty());
    }

    @Test
    void prunesExpiredSessions() {
        AuthStore store = store();
        Session valid = store.createSession("alice", Role.ADMIN, 3600);
        store.createSession("alice", Role.ADMIN, -10);
        assertEquals(1, store.pruneExpiredSessions());
        assertTrue(store.getSession(valid.token()).isPresent());
    }

    @Test
    void deletingUserClearsSessions() {
        AuthStore store = store();
        store.createUser("alice", "password123", Role.ADMIN);
        Session session = store.createSession("alice", Role.ADMIN);
        store.deleteUser("alice");
        assertEquals(0, store.countUsers());
        assertTrue(store.getSession(session.token()).isEmpty());
    }

    @Test
    void oauthBootstrapRolePrecedence() {
        // Unverified / missing email never becomes admin.
        assertEquals(Role.VIEWER, AuthStore.oauthBootstrapRole("a@x.com", false, List.of()));
        assertEquals(Role.VIEWER, AuthStore.oauthBootstrapRole(null, true, List.of()));
        // Admin-email list is the only path to admin (case-insensitive).
        assertEquals(Role.ADMIN, AuthStore.oauthBootstrapRole("A@X.com", true, List.of("a@x.com")));
        assertEquals(Role.VIEWER, AuthStore.oauthBootstrapRole("b@x.com", true, List.of("a@x.com")));
        // No admin list: everyone — including the first user — is a viewer.
        assertEquals(Role.VIEWER, AuthStore.oauthBootstrapRole("c@x.com", true, List.of()));
    }

    @Test
    void getOrCreateOauthUserProvisionsThenRefreshes() {
        AuthStore store = store();
        User created = store.getOrCreateOauthUser("google", "123", "a@x.com", "Ann", true, List.of("a@x.com"));
        assertEquals("google:123", created.username());
        assertEquals(Role.ADMIN.wire(), created.role()); // allowlisted
        assertTrue(created.isOauth());

        User refreshed = store.getOrCreateOauthUser("google", "123", "new@x.com", "Ann N", true, List.of("z@x.com"));
        assertEquals("google:123", refreshed.username());
        assertEquals(Role.ADMIN.wire(), refreshed.role()); // role preserved
        assertEquals("new@x.com", refreshed.email());
        assertEquals(1, store.countUsers());
    }

    @Test
    void envBootstrapIsIdempotent() {
        AuthStore store = store();
        Map<String, String> env = Map.of(
                AuthStore.ENV_ADMIN_USER, "root",
                AuthStore.ENV_ADMIN_PASSWORD, "password123");
        store.bootstrapAdminFromEnv(env);
        assertEquals(1, store.countUsers());
        store.bootstrapAdminFromEnv(env);
        assertEquals(1, store.countUsers());
        assertNotNull(store.authenticate("root", "password123"));

        AuthStore empty = store();
        empty.bootstrapAdminFromEnv(Map.of());
        assertEquals(0, empty.countUsers());
    }

    @Test
    void changePasswordInvalidatesOldCredential() {
        AuthStore store = store();
        store.createUser("alice", "password123", Role.ADMIN);
        Session existing = store.createSession("alice", Role.ADMIN);
        store.updatePassword("alice", "new-password!");
        assertNull(store.authenticate("alice", "password123"));
        assertNotNull(store.authenticate("alice", "new-password!"));
        // Sessions from before the change are revoked.
        assertTrue(store.getSession(existing.token()).isEmpty());
        assertFalse(store.getSession("missing").isPresent());
    }
}
