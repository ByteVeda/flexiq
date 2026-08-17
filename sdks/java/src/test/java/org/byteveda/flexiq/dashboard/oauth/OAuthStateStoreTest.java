package org.byteveda.flexiq.dashboard.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.auth.oauth.OAuthStateStore;
import org.byteveda.flexiq.dashboard.auth.oauth.model.OAuthState;
import org.byteveda.flexiq.dashboard.support.Json;
import org.junit.jupiter.api.Test;

class OAuthStateStoreTest {

    private final InMemorySettings settings = new InMemorySettings();
    private final OAuthStateStore store = new OAuthStateStore(settings);

    @Test
    void createThenConsumeRoundTrips() {
        OAuthState created = store.create("google", "/dashboard");
        Optional<OAuthState> consumed = store.consume(created.state());
        assertTrue(consumed.isPresent());
        OAuthState row = consumed.get();
        assertEquals(created.state(), row.state());
        assertEquals(created.nonce(), row.nonce());
        assertEquals(created.codeVerifier(), row.codeVerifier());
        assertEquals("google", row.slot());
        assertEquals("/dashboard", row.nextUrl());
    }

    @Test
    void consumeIsSingleUse() {
        OAuthState created = store.create("github", "/");
        assertTrue(store.consume(created.state()).isPresent());
        // Replay of the same state finds nothing — the row was deleted on first use.
        assertTrue(store.consume(created.state()).isEmpty());
    }

    @Test
    void expiredStateIsNotReturned() {
        long past = System.currentTimeMillis() / 1000 - 10;
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("nonce", "n");
        row.put("code_verifier", "v");
        row.put("slot", "google");
        row.put("next_url", "/");
        row.put("created_at", past - 300);
        row.put("expires_at", past);
        settings.setSetting(OAuthStateStore.STATE_PREFIX + "expired", Json.toString(row));

        assertTrue(store.consume("expired").isEmpty());
        // The lookup still deletes the row (delete-before-parse).
        assertFalse(
                settings.getSetting(OAuthStateStore.STATE_PREFIX + "expired").isPresent());
    }

    @Test
    void malformedRowYieldsEmpty() {
        settings.setSetting(OAuthStateStore.STATE_PREFIX + "bad", "not-json");
        assertTrue(store.consume("bad").isEmpty());
        assertTrue(store.consume(null).isEmpty());
        assertTrue(store.consume("missing").isEmpty());
    }

    @Test
    void pruneExpiredRemovesOnlyStaleRows() {
        store.create("google", "/"); // fresh, TTL 300s
        long past = System.currentTimeMillis() / 1000 - 10;
        Map<String, Object> stale = new LinkedHashMap<>();
        stale.put("expires_at", past);
        settings.setSetting(OAuthStateStore.STATE_PREFIX + "stale", Json.toString(stale));

        assertEquals(1, store.pruneExpired());
        assertFalse(settings.getSetting(OAuthStateStore.STATE_PREFIX + "stale").isPresent());
    }
}
