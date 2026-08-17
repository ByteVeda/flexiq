package org.byteveda.flexiq.dashboard.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.HexFormat;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * PBKDF2-HMAC-SHA256 password hashing, byte-compatible across SDKs.
 *
 * <p>Stored format (hex, not base64): {@code pbkdf2_sha256$<iters>$<salt_hex>$<hash_hex>}
 * — 600k iterations, 16-byte salt, 32-byte derived key. The JDK's
 * {@code PBKDF2WithHmacSHA256} encodes the password as UTF-8, matching the
 * reference implementation for both ASCII and non-ASCII passwords.
 *
 * <p>OAuth users store the sentinel {@code oauth:<slot>} instead of a hash;
 * {@link #verify} rejects it so password login is impossible for them.
 */
final class PasswordHasher {
    static final String SCHEME = "pbkdf2_sha256";
    static final int ITERATIONS = 600_000;
    static final int SALT_BYTES = 16;
    static final int HASH_BYTES = 32;
    static final String OAUTH_PREFIX = "oauth:";

    /** Fixed hash verified against unknown usernames to equalise login timing. */
    private static final String DUMMY_HASH =
            SCHEME + "$" + ITERATIONS + "$" + "0".repeat(SALT_BYTES * 2) + "$" + "0".repeat(HASH_BYTES * 2);

    private static final HexFormat HEX = HexFormat.of();
    private static final SecureRandom RANDOM = new SecureRandom();

    private PasswordHasher() {}

    static String hash(String password) {
        byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        byte[] derived = pbkdf2(password, salt, ITERATIONS, HASH_BYTES * 8);
        return SCHEME + "$" + ITERATIONS + "$" + HEX.formatHex(salt) + "$" + HEX.formatHex(derived);
    }

    static boolean verify(String password, String encoded) {
        if (encoded == null || encoded.startsWith(OAUTH_PREFIX)) {
            // Still pay a derivation so an OAuth-only or unknown user can't be
            // distinguished from a password user by response timing.
            dummyVerify(password);
            return false;
        }
        String[] parts = encoded.split("\\$", -1);
        if (parts.length != 4 || !SCHEME.equals(parts[0])) {
            return false;
        }
        int iterations;
        byte[] salt;
        byte[] expected;
        try {
            iterations = Integer.parseInt(parts[1]);
            salt = HEX.parseHex(parts[2]);
            expected = HEX.parseHex(parts[3]);
        } catch (IllegalArgumentException e) {
            return false;
        }
        if (iterations <= 0 || salt.length == 0 || expected.length == 0) {
            return false;
        }
        byte[] candidate;
        try {
            // Re-derive to the STORED hash length, mirroring the reference verifier.
            candidate = pbkdf2(password, salt, iterations, expected.length * 8);
        } catch (RuntimeException e) {
            return false;
        }
        return MessageDigest.isEqual(candidate, expected);
    }

    /** Run a dummy derivation so an unknown user costs the same as a known one. */
    static void dummyVerify(String password) {
        verify(password, DUMMY_HASH);
    }

    static String oauthSentinel(String slot) {
        return OAUTH_PREFIX + slot;
    }

    static boolean isOauth(String passwordHash) {
        return passwordHash != null && passwordHash.startsWith(OAUTH_PREFIX);
    }

    private static byte[] pbkdf2(String password, byte[] salt, int iterations, int keyLengthBits) {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, keyLengthBits);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(spec)
                    .getEncoded();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA256 unavailable", e);
        } catch (InvalidKeySpecException e) {
            // Empty password: the reference min length is 8, so this is a misuse.
            throw new IllegalArgumentException("invalid password spec", e);
        } finally {
            spec.clearPassword();
        }
    }
}
