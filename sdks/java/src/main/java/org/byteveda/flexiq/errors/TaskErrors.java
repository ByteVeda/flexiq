package org.byteveda.flexiq.errors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Codec for the canonical cross-SDK task-error JSON
 * ({@code {"errtype":...,"message":...,"traceback":[...]}}, keys in that order,
 * no extra whitespace) stored verbatim in job, error-history, and dead-letter
 * {@code error} fields. Core-generated maintenance errors (timeout, expiry,
 * worker-death recovery) stay plain strings by design, so readers must fall
 * back — {@link #decode(String)} returns {@code null} for those.
 */
public final class TaskErrors {
    private static final ObjectMapper JSON = new ObjectMapper();

    private TaskErrors() {}

    /**
     * Encode a thrown task error: fully-qualified class name, verbatim message, stack frames in order.
     *
     * @param error what the handler threw
     * @return the canonical JSON, ready to store in an {@code error} field
     */
    public static String encode(Throwable error) {
        ObjectNode node = JSON.createObjectNode();
        node.put("errtype", error.getClass().getName());
        String message = error.getMessage();
        node.put("message", message == null ? "" : message);
        ArrayNode traceback = node.putArray("traceback");
        for (StackTraceElement frame : error.getStackTrace()) {
            traceback.add(frame.toString());
        }
        return node.toString();
    }

    /**
     * Decode a stored error string, e.g. {@link org.byteveda.flexiq.events.OutcomeEvent#error}
     * or {@link org.byteveda.flexiq.model.DeadJob#error}. Returns {@code null} unless the
     * string is a JSON object with a string {@code message} — the signal that it is plain
     * legacy/system text and must be surfaced as-is.
     *
     * @param raw the stored error string
     * @return the decoded error, or {@code null} when the string is plain text to
     *     be shown verbatim
     */
    public static @Nullable TaskError decode(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        JsonNode node = readTree(raw);
        if (node == null || !node.isObject()) {
            return null;
        }
        JsonNode message = node.get("message");
        if (message == null || !message.isTextual()) {
            return null;
        }
        // Default a missing errtype to "Error" to match the cross-SDK contract
        // (other SDKs decode it identically), so the same JSON summarizes the
        // same way everywhere.
        JsonNode errtype = node.get("errtype");
        String errtypeText = errtype != null && errtype.isTextual() ? errtype.asText() : "Error";
        return new TaskError(errtypeText, message.asText(), readTraceback(node.get("traceback")), raw);
    }

    /**
     * One-line human summary: {@code errtype: message} when structured, the raw string otherwise.
     *
     * @param raw the stored error string
     * @return the summary, safe to show whether or not the string was structured
     */
    public static String summarize(String raw) {
        TaskError decoded = decode(raw);
        return decoded == null ? raw : decoded.summary();
    }

    private static @Nullable JsonNode readTree(String raw) {
        try {
            return JSON.readTree(raw);
        } catch (IOException e) {
            return null;
        }
    }

    private static List<String> readTraceback(JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<String> frames = new ArrayList<>(node.size());
        for (JsonNode frame : node) {
            if (frame.isTextual()) {
                frames.add(frame.asText());
            }
        }
        return frames;
    }
}
