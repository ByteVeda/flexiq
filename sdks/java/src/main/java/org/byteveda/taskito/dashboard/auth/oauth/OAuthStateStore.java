package org.byteveda.taskito.dashboard.auth.oauth;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.byteveda.taskito.dashboard.auth.Tokens;
import org.byteveda.taskito.dashboard.auth.oauth.model.OAuthState;
import org.byteveda.taskito.dashboard.store.SettingsAccess;
import org.byteveda.taskito.dashboard.support.Json;

/**
 * Create, consume (read+delete), and prune short-lived OAuth state rows.
 *
 * <p>Rows live in the settings KV under the {@code auth:oauth_state:<state>}
 * namespace alongside sessions, so they work uniformly across SQLite / Postgres
 * / Redis with no new migrations. Each row carries the {@code nonce}, PKCE
 * {@code code_verifier}, target {@code slot}, and post-login {@code next_url};
 * the {@code state} token itself is the key suffix and is not stored.
 *
 * <p><b>Single-use invariant:</b> {@link #consume} always deletes the row before
 * parsing, so a replayed {@code state} finds nothing — even if the row is
 * malformed.
 */
public final class OAuthStateStore {
    public static final String STATE_PREFIX = "auth:oauth_state:";
    /** 5 min — covers consent UX plus reasonable network latency. */
    public static final long DEFAULT_STATE_TTL_SECONDS = 5 * 60;

    private final SettingsAccess settings;
    private final AtomicLong lastPruneAt = new AtomicLong(0);

    public OAuthStateStore(SettingsAccess settings) {
        this.settings = settings;
    }

    /** Mint a fresh state/nonce/verifier triple, persist it, and return it. */
    public OAuthState create(String slot, String nextUrl) {
        long now = nowSeconds();
        OAuthState state = new OAuthState(
                Tokens.urlSafe(Tokens.STATE_TOKEN_BYTES),
                Tokens.urlSafe(Tokens.NONCE_BYTES),
                Tokens.urlSafe(Tokens.CODE_VERIFIER_BYTES),
                slot,
                nextUrl,
                now,
                now + DEFAULT_STATE_TTL_SECONDS);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("nonce", state.nonce());
        row.put("code_verifier", state.codeVerifier());
        row.put("slot", state.slot());
        row.put("next_url", state.nextUrl());
        row.put("created_at", state.createdAt());
        row.put("expires_at", state.expiresAt());
        settings.setSetting(STATE_PREFIX + state.state(), Json.toString(row));
        return state;
    }

    /**
     * Look up {@code stateToken} and delete it atomically. Empty if missing,
     * malformed, or expired. The delete happens <b>before</b> parsing so a replay
     * of the same state never re-validates.
     */
    public Optional<OAuthState> consume(String stateToken) {
        if (stateToken == null || stateToken.isEmpty()) {
            return Optional.empty();
        }
        String key = STATE_PREFIX + stateToken;
        Optional<String> raw = settings.getSetting(key);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        // Delete first: any subsequent request with the same state sees nothing.
        settings.deleteSetting(key);
        Map<String, Object> data = Json.parseMap(raw.get());
        if (data == null) {
            return Optional.empty();
        }
        OAuthState state;
        try {
            state = new OAuthState(
                    stateToken,
                    Json.requireString(data, "nonce"),
                    Json.requireString(data, "code_verifier"),
                    Json.requireString(data, "slot"),
                    Json.optionalString(data, "next_url"),
                    Json.requireLong(data, "created_at"),
                    Json.requireLong(data, "expires_at"));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (state.isExpired(nowSeconds())) {
            return Optional.empty();
        }
        return Optional.of(state);
    }

    /**
     * Sweep at most once per TTL window so the O(n) KV scan stays off the
     * per-login hot path. Single-use consume + expiry check already bound the
     * garbage; this only reclaims rows from abandoned flows.
     */
    public void pruneExpiredIfDue() {
        long now = nowSeconds();
        long last = lastPruneAt.get();
        if (now - last < DEFAULT_STATE_TTL_SECONDS) {
            return;
        }
        if (lastPruneAt.compareAndSet(last, now)) {
            pruneExpired();
        }
    }

    /** Best-effort sweep of expired state rows. Returns the count removed. */
    public int pruneExpired() {
        long now = nowSeconds();
        int removed = 0;
        for (Map.Entry<String, String> entry : settings.listSettings().entrySet()) {
            if (!entry.getKey().startsWith(STATE_PREFIX)) {
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

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }
}
