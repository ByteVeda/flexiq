package org.byteveda.flexiq.dashboard.auth;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * Cryptographically strong opaque tokens. Encoding matches the other SDKs'
 * {@code secrets.token_urlsafe(n)} / {@code randomBytes(n).toString("base64url")}:
 * {@code n} random bytes rendered as unpadded URL-safe base64 (32 bytes → 43
 * chars, 16 bytes → 22 chars).
 */
public final class Tokens {
    public static final int SESSION_TOKEN_BYTES = 32;
    public static final int NONCE_BYTES = 16;
    public static final int STATE_TOKEN_BYTES = 32;
    public static final int CODE_VERIFIER_BYTES = 32;

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Base64.Encoder URL_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private Tokens() {}

    public static String urlSafe(int numBytes) {
        byte[] bytes = new byte[numBytes];
        RANDOM.nextBytes(bytes);
        return URL_ENCODER.encodeToString(bytes);
    }

    /** A 256-bit session or CSRF token. */
    public static String session() {
        return urlSafe(SESSION_TOKEN_BYTES);
    }
}
