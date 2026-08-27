package org.byteveda.flexiq.test;

import org.byteveda.flexiq.steps.StepError;

/**
 * Step identity for the in-memory harness: how a name plus an occurrence — or a
 * name plus an explicit key — becomes the string a memo lookup matches on.
 *
 * <p>A Java restatement of the core's rules, because this harness is JNI-free by
 * construction and so cannot ask the core. What keeps it from drifting is
 * {@code StepHarnessParityTest}, which runs one body against this backend and
 * against a real worker and asserts they agree; that check is the reason this
 * class is allowed to exist.
 */
final class StepKeys {

    /**
     * Longest a step name may be. A name is written by hand at the call site; a
     * limit this generous only ever catches a name built from data by mistake.
     */
    private static final int MAX_NAME_BYTES = 128;

    /**
     * Longest an explicit key may be. Keys <i>are</i> built from data — an order
     * id, a tenant — so they get more room than a name.
     */
    private static final int MAX_KEY_BYTES = 256;

    /** Separates a name from its occurrence counter. */
    private static final char OCCURRENCE_SEPARATOR = '#';

    /** Separates a name from an explicit key. */
    private static final char KEY_SEPARATOR = ':';

    /** Longest prefix an error message quotes back. */
    private static final int MAX_QUOTED_CHARS = 48;

    private StepKeys() {}

    /**
     * {@code name#occurrence} — the default identity, where {@code occurrence} is
     * how many times this name has already been requested in this attempt.
     *
     * <p>Stable only while the surrounding code requests the same names in the
     * same order, which is exactly what the divergence check verifies.
     */
    static String derive(String name, int occurrence) {
        validateName(name);
        return name + OCCURRENCE_SEPARATOR + occurrence;
    }

    /**
     * {@code name:key} — identity pinned to the data rather than to the position.
     *
     * <p>A key is only ever compared, never parsed back, so it may contain
     * anything the caller likes, including the separators.
     */
    static String explicit(String name, String key) {
        validateName(name);
        validateKey(name, key);
        return name + KEY_SEPARATOR + key;
    }

    /**
     * A name must be writable as itself in a key, so it may hold neither
     * separator: {@code charge#1} as a name would collide with the second
     * occurrence of {@code charge}.
     */
    private static void validateName(String name) {
        if (name == null || name.isEmpty()) {
            throw refuse("a step name must not be empty");
        }
        int bytes = utf8Length(name);
        if (bytes > MAX_NAME_BYTES) {
            throw refuse("step name '" + abbreviate(name) + "' is " + bytes + " bytes, over the " + MAX_NAME_BYTES
                    + " byte limit");
        }
        for (char separator : new char[] {OCCURRENCE_SEPARATOR, KEY_SEPARATOR}) {
            if (name.indexOf(separator) >= 0) {
                throw refuse("step name '" + abbreviate(name) + "' contains '" + separator
                        + "', which separates a name from its key");
            }
        }
    }

    private static void validateKey(String name, String key) {
        if (key.isEmpty()) {
            // Never a fallback to numbering by occurrence: an empty key is a
            // caller mistake, and answering it with a positional identity is how
            // a harness passes a test the real run rejects.
            throw refuse(
                    "step '" + abbreviate(name) + "' was given an empty key; omit the key to number it by occurrence");
        }
        int bytes = utf8Length(key);
        if (bytes > MAX_KEY_BYTES) {
            throw refuse("key '" + abbreviate(key) + "' of step '" + abbreviate(name) + "' is " + bytes
                    + " bytes, over the " + MAX_KEY_BYTES + " byte limit");
        }
    }

    /**
     * A name or key this attempt cannot use is written that way in the code, so
     * the replay is handed the same value: permanent, and the retry budget must
     * not be spent on it.
     */
    private static StepError refuse(String message) {
        return new StepError(message, false);
    }

    /** Bytes the core measures — it validates a UTF-8 encoding, not a char count. */
    private static int utf8Length(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    /**
     * Bound what an error message quotes back. The value that failed is often the
     * reason it failed — a name built from a payload — and pasting all of it into
     * a log line helps nobody.
     */
    static String abbreviate(String value) {
        // By code point, never by char: a cut through a surrogate pair renders as
        // a replacement character in the very message meant to identify the value.
        if (value.codePointCount(0, value.length()) <= MAX_QUOTED_CHARS) {
            return value;
        }
        return value.substring(0, value.offsetByCodePoints(0, MAX_QUOTED_CHARS)) + "…";
    }
}
