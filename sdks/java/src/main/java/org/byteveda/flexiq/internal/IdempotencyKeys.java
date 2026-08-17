package org.byteveda.flexiq.internal;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Derives the automatic idempotency {@code uniqueKey} for a task from its name and
 * serialized payload. The recipe is a cross-SDK wire contract — peer SDKs compute the
 * identical key for the same {@code (name, payload)} so idempotent enqueues dedupe across
 * languages:
 *
 * <pre>{@code
 * uniqueKey = "auto:" + sha256( utf8(name) ++ 0x00 ++ payload ).hex().substring(0, 32)
 * }</pre>
 *
 * <p>The separator is a single {@code NUL} byte (not a printable delimiter) so a task name
 * cannot be confused with the start of a payload. The hash is taken over the <em>pre-codec</em>
 * payload bytes, so a non-deterministic codec (e.g. an AES-GCM nonce) cannot break dedup.
 */
public final class IdempotencyKeys {
    private static final String PREFIX = "auto:";
    private static final int KEY_HEX_CHARS = 32;

    private IdempotencyKeys() {}

    /** The {@code auto:}-prefixed idempotency key for {@code taskName} over {@code payload}. */
    public static String autoKey(String taskName, byte[] payload) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated on every conformant JVM; its absence is unrecoverable.
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        digest.update(taskName.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0x00);
        digest.update(payload);
        String hex = HexFormat.of().formatHex(digest.digest());
        return PREFIX + hex.substring(0, KEY_HEX_CHARS);
    }
}
