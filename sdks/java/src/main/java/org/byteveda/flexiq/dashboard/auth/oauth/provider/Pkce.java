package org.byteveda.flexiq.dashboard.auth.oauth.provider;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.byteveda.flexiq.dashboard.auth.Tokens;

/**
 * PKCE (RFC 7636) code-verifier / S256 code-challenge derivation.
 *
 * <p>A fresh high-entropy verifier is minted per flow and stashed server-side;
 * only its {@code S256} challenge travels to the provider. The verifier is
 * replayed on the token exchange, binding the auth code to this browser and
 * defeating code-interception attacks.
 */
public final class Pkce {
    /** Always {@code S256} — the only method OAuth providers uniformly support. */
    public static final String METHOD = "S256";

    private Pkce() {}

    /** A 43-char base64url verifier (32 random bytes), above the RFC 7636 minimum. */
    public static String verifier() {
        return Tokens.urlSafe(Tokens.CODE_VERIFIER_BYTES);
    }

    /**
     * The {@code S256} challenge for {@code verifier}: base64url (no padding) of
     * {@code SHA-256(verifier)}, matching every provider's PKCE implementation.
     */
    public static String challenge(String verifier) {
        byte[] digest = sha256(verifier.getBytes(StandardCharsets.US_ASCII));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    }

    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
