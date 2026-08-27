package org.byteveda.flexiq.dashboard.support;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.errors.SerializationException;
import org.jspecify.annotations.Nullable;

/**
 * Shared JSON codec for the dashboard. Output is compact (no spaces) so that
 * records written to the settings KV are byte-compatible with the other SDKs,
 * which persist with the equivalent of {@code json.dumps(separators=(",", ":"))}.
 */
public final class Json {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private Json() {}

    /**
     * Encode a response body.
     *
     * @param value the body
     * @return its compact JSON bytes
     */
    public static byte[] toBytes(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new SerializationException("failed to encode response", e);
        }
    }

    /**
     * Encode a value for the settings KV.
     *
     * @param value the record to store
     * @return its compact JSON, byte-compatible with what the other SDKs write
     */
    public static String toString(@Nullable Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new SerializationException("failed to encode value", e);
        }
    }

    /**
     * Parse an object body; returns {@code null} for non-object or malformed input.
     *
     * @param body the request body, or {@code null}
     * @return the parsed map, or {@code null} — the caller decides which status that is
     */
    public static @Nullable Map<String, Object> readObject(byte @Nullable [] body) {
        if (body == null || body.length == 0) {
            return null;
        }
        try {
            return asMap(MAPPER.readTree(body));
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Parse a stored JSON string into a mutable map; {@code null} if malformed/non-object.
     *
     * @param json the stored record, or {@code null}
     * @return the parsed map, or {@code null} — a corrupt row reads as absent rather
     *     than failing the request
     */
    public static @Nullable Map<String, Object> parseMap(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return asMap(MAPPER.readTree(json));
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Parse a JSON array of strings; empty list if malformed/non-array/null.
     *
     * @param json the stored array, or {@code null}
     * @return the strings, or an empty list — a corrupt row reads as absent
     */
    public static List<String> parseStringList(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return List.of();
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            if (node == null || !node.isArray()) {
                return List.of();
            }
            List<String> out = new ArrayList<>(node.size());
            node.forEach(element -> out.add(element.asText()));
            return out;
        } catch (IOException e) {
            return List.of();
        }
    }

    /**
     * Parse a JSON array of objects into maps; empty list if malformed/non-array/null.
     *
     * @param json the stored array, or {@code null}
     * @return the objects, non-object elements dropped; empty when the row is corrupt
     */
    public static List<Map<String, Object>> parseListOfObjects(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return List.of();
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            if (node == null || !node.isArray()) {
                return List.of();
            }
            List<Map<String, Object>> out = new ArrayList<>(node.size());
            for (JsonNode element : node) {
                Map<String, Object> map = asMap(element);
                if (map != null) {
                    out.add(map);
                }
            }
            return out;
        } catch (IOException e) {
            return List.of();
        }
    }

    /**
     * A required string field of a parsed record; throws when absent or not a string.
     *
     * @param data the parsed record
     * @param key the field's name
     * @return the value
     */
    public static String requireString(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof String text) {
            return text;
        }
        throw new IllegalArgumentException("expected a string at '" + key + "', got " + value);
    }

    /**
     * An optional string field of a parsed record; {@code null} when absent or not a string.
     *
     * @param data the parsed record
     * @param key the field's name
     * @return the value, or {@code null}
     */
    public static @Nullable String optionalString(Map<String, Object> data, String key) {
        return data.get(key) instanceof String text ? text : null;
    }

    /**
     * A required numeric field of a parsed record; throws when absent or not a number.
     *
     * @param data the parsed record
     * @param key the field's name
     * @return the value
     */
    public static long requireLong(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalArgumentException("expected a number at '" + key + "', got " + value);
    }

    /**
     * An optional numeric field of a parsed record; {@code null} when absent or not a number.
     *
     * @param data the parsed record
     * @param key the field's name
     * @return the value, or {@code null}
     */
    public static @Nullable Long optionalLong(Map<String, Object> data, String key) {
        return data.get(key) instanceof Number number ? number.longValue() : null;
    }

    private static @Nullable Map<String, Object> asMap(@Nullable JsonNode node) {
        if (node == null || !node.isObject()) {
            return null;
        }
        return MAPPER.convertValue(node, MAP_TYPE);
    }
}
