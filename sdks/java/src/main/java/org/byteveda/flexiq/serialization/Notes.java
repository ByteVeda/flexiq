package org.byteveda.flexiq.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.errors.NotesValidationException;
import org.jspecify.annotations.Nullable;

/**
 * Validation and canonical encoding for the structured {@code notes} field on a job.
 *
 * <p>Notes are a small, bounded, user-readable annotation map attached at enqueue time and
 * stored as a JSON string in the {@code jobs.notes} column. Validation lives in the SDK (the
 * single boundary every enqueue funnels through) so the native layer stays schema-agnostic and
 * stores the encoded string verbatim. The bounds keep a note set renderable as a fixed-size
 * key/value table without truncation.
 *
 * <p>Contract: at most {@link #MAX_FIELDS} top-level keys; each key a non-empty string no longer
 * than {@link #MAX_KEY_LENGTH} characters; values are JSON primitives, lists, or maps nested at
 * most {@link #MAX_DEPTH} levels; string leaves at most {@link #MAX_VALUE_LENGTH} characters; and
 * the encoded document at most {@link #MAX_BYTES} UTF-8 bytes.
 */
public final class Notes {
    /** Most top-level keys one note map may carry. */
    public static final int MAX_FIELDS = 15;

    /** Longest a key may be, in characters. */
    public static final int MAX_KEY_LENGTH = 64;

    /** Longest a string leaf may be, in characters. */
    public static final int MAX_VALUE_LENGTH = 500;

    /** Deepest a nested list or map may go. */
    public static final int MAX_DEPTH = 3;

    /** Largest the encoded document may be, in UTF-8 bytes. */
    public static final int MAX_BYTES = 4096;

    // Canonical form: keys sorted, compact separators (Jackson's default), non-ASCII emitted raw.
    private static final ObjectWriter CANONICAL = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .writer();

    private Notes() {}

    /**
     * Validate {@code notes} and return its canonical JSON encoding. A {@code null} map returns
     * {@code null} (the absence of notes, distinct from an empty map which encodes as {@code "{}"}).
     *
     * @param notes the annotations to attach, or {@code null} for none
     * @return the canonical JSON, or {@code null} when {@code notes} was {@code null}
     * @throws NotesValidationException if any bound in the class contract is violated
     */
    public static @Nullable String encode(Map<String, ?> notes) {
        if (notes == null) {
            return null;
        }
        if (notes.size() > MAX_FIELDS) {
            throw new NotesValidationException(
                    "notes may not have more than " + MAX_FIELDS + " fields, got " + notes.size());
        }
        for (Map.Entry<String, ?> entry : notes.entrySet()) {
            validateKey(entry.getKey());
            validateValue(entry.getKey(), entry.getValue(), 1);
        }
        String encoded;
        try {
            encoded = CANONICAL.writeValueAsString(notes);
        } catch (JsonProcessingException e) {
            throw new NotesValidationException("notes are not serializable: " + e.getMessage(), e);
        }
        int bytes = encoded.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_BYTES) {
            throw new NotesValidationException(
                    "encoded notes are " + bytes + " bytes, exceeds limit of " + MAX_BYTES + " bytes");
        }
        return encoded;
    }

    private static void validateKey(Object key) {
        if (!(key instanceof String)) {
            throw new NotesValidationException("note keys must be strings, got " + typeName(key));
        }
        String k = (String) key;
        if (k.isEmpty()) {
            throw new NotesValidationException("note keys may not be empty strings");
        }
        if (k.length() > MAX_KEY_LENGTH) {
            throw new NotesValidationException(
                    "note key '" + k + "' is " + k.length() + " characters, exceeds limit of " + MAX_KEY_LENGTH);
        }
    }

    private static void validateValue(String path, Object value, int depth) {
        if (depth > MAX_DEPTH) {
            throw new NotesValidationException(
                    "note value at '" + path + "' exceeds max nesting depth of " + MAX_DEPTH);
        }
        if (value == null || value instanceof Boolean) {
            return;
        }
        if (value instanceof String s) {
            if (s.length() > MAX_VALUE_LENGTH) {
                throw new NotesValidationException("note value at '" + path + "' is " + s.length()
                        + " characters, exceeds limit of " + MAX_VALUE_LENGTH);
            }
            return;
        }
        if (value instanceof Double || value instanceof Float) {
            if (!Double.isFinite(((Number) value).doubleValue())) {
                // NaN/Infinity serialize to bare tokens that strict JSON readers reject.
                throw new NotesValidationException(
                        "note value at '" + path + "' must be a finite number, got " + value);
            }
            return;
        }
        if (value instanceof Number) {
            return;
        }
        if (value instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                validateValue(path + "[" + i + "]", list.get(i), depth + 1);
            }
            return;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                validateKey(entry.getKey());
                validateValue(path + "." + entry.getKey(), entry.getValue(), depth + 1);
            }
            return;
        }
        throw new NotesValidationException("note value at '" + path + "' has unsupported type " + typeName(value)
                + "; must be a string, number, boolean, null, list, or map");
    }

    private static String typeName(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName();
    }
}
