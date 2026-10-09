package org.byteveda.flexiq.contrib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.jspecify.annotations.Nullable;

/**
 * W3C trace context carried in a job's metadata, by the cross-SDK contract.
 *
 * <p>The carrier is the metadata JSON object itself: {@code traceparent} and
 * {@code tracestate} at its top level. Every SDK merges by the same rule —
 * absent or blank metadata becomes an object holding just the carrier; an object
 * gains the carrier keys unless it already names either one, and never loses or
 * overwrites a key of its own; anything else is left untouched. An object's own
 * bytes are kept: the keys are spliced in after its opening brace.
 */
final class TraceCarrier {
    static final String TRACEPARENT = "traceparent";
    static final String TRACESTATE = "tracestate";

    // Trailing tokens refused, so `{"a":1} junk` is not mistaken for an object.
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private TraceCarrier() {}

    /**
     * {@code metadata} with {@code carrier}'s keys added, by the rule above.
     *
     * @param metadata the blob the caller set, if any
     * @param carrier what the propagator injected
     * @return the merged blob, or {@code metadata} unchanged
     */
    static @Nullable String merge(@Nullable String metadata, Map<String, String> carrier) {
        if (carrier.isEmpty()) {
            return metadata;
        }
        if (metadata == null || metadata.isBlank()) {
            return "{" + entries(carrier, Set.of()) + "}";
        }
        JsonNode existing = parse(metadata);
        if (existing == null || !existing.isObject() || existing.has(TRACEPARENT) || existing.has(TRACESTATE)) {
            return metadata;
        }
        Set<String> taken = new HashSet<>();
        existing.fieldNames().forEachRemaining(taken::add);
        String entries = entries(carrier, taken);
        if (entries.isEmpty()) {
            return metadata;
        }
        // A parsed object's first non-whitespace character is its `{`.
        int brace = metadata.length() - metadata.stripLeading().length() + 1;
        String separator = existing.isEmpty() ? "" : ",";
        return metadata.substring(0, brace) + entries + separator + metadata.substring(brace);
    }

    /**
     * The string members of a job's metadata, for a propagator to read.
     *
     * @param metadata the job's parsed metadata
     * @return its string-valued entries; empty when it has none
     */
    static Map<String, String> extract(Map<String, Object> metadata) {
        Map<String, String> carrier = new LinkedHashMap<>();
        metadata.forEach((key, value) -> {
            if (value instanceof String text) {
                carrier.put(key, text);
            }
        });
        return carrier;
    }

    private static @Nullable JsonNode parse(String metadata) {
        try {
            return JSON.readTree(metadata);
        } catch (JsonProcessingException notJson) {
            // Free-form text is valid metadata; it just cannot carry a context.
            return null;
        }
    }

    /** The carrier's keys not in {@code taken}, as JSON object members. */
    private static String entries(Map<String, String> carrier, Set<String> taken) {
        StringJoiner joined = new StringJoiner(",");
        carrier.forEach((key, value) -> {
            if (!taken.contains(key)) {
                // TextNode renders as a quoted, escaped JSON string.
                joined.add(new TextNode(key) + ":" + new TextNode(value));
            }
        });
        return joined.toString();
    }
}
