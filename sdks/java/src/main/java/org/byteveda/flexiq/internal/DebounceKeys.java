package org.byteveda.flexiq.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Resolves a debounce key template against the payload of one enqueue.
 *
 * <p>A template names the window's identity, e.g. {@code "report:{userId}"}: the literal
 * text is kept and each {@code {…}} placeholder is replaced by that property of the
 * enqueued payload. A dotted placeholder ({@code "{owner.id}"}) walks into a nested
 * object. A template with no placeholder resolves to itself — a deliberate single window
 * for the whole task.
 *
 * <p>Every failure throws instead of degrading to a partially-resolved or global key:
 * debouncing every user's report against every other user's is a data bug that would only
 * ever surface as mysteriously missing runs.
 *
 * <p>Resolution reads the payload through a private {@link ObjectMapper}, not the client's
 * configured {@code Serializer}: the key must be derived the same way whatever wire format
 * the task uses, and a JSON tree gives records, beans and {@code Map}s one spelling.
 */
public final class DebounceKeys {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([^{}]*)}");

    private DebounceKeys() {}

    /**
     * The concrete key for {@code payload}, or the template itself when it has no
     * placeholder. {@code taskName} only names the task in error messages.
     *
     * @throws IllegalArgumentException if a placeholder is empty, names something the
     *     payload does not carry, or resolves to a value that cannot key a window
     */
    public static String resolve(String template, String taskName, @Nullable Object payload) {
        Matcher matcher = PLACEHOLDER.matcher(template);
        if (!matcher.find()) {
            return template;
        }
        JsonNode tree = tree(template, taskName, payload);
        StringBuilder out = new StringBuilder();
        do {
            String value = lookup(matcher.group(1), template, taskName, tree);
            matcher.appendReplacement(out, Matcher.quoteReplacement(value));
        } while (matcher.find());
        matcher.appendTail(out);
        String key = out.toString();
        if (key.isEmpty()) {
            throw new IllegalArgumentException(
                    "debounceKey \"" + template + "\" for task '" + taskName + "' resolved to an empty key");
        }
        return key;
    }

    /** The payload as a JSON tree, rejecting the shapes no placeholder could read. */
    private static JsonNode tree(String template, String taskName, @Nullable Object payload) {
        if (payload == null) {
            throw new IllegalArgumentException("debounceKey \"" + template + "\" for task '" + taskName
                    + "' has placeholders but the payload is null");
        }
        JsonNode tree;
        try {
            tree = MAPPER.valueToTree(payload);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "debounceKey \"" + template + "\" for task '" + taskName + "' cannot read the payload: "
                            + e.getMessage(),
                    e);
        }
        if (!tree.isObject()) {
            throw new IllegalArgumentException("debounceKey \"" + template + "\" for task '" + taskName
                    + "' names payload properties, but the payload serializes to "
                    + tree.getNodeType().toString().toLowerCase(java.util.Locale.ROOT)
                    + " — use a literal key, or enqueue an object payload");
        }
        return tree;
    }

    /** One placeholder's value, as the text that goes into the key. */
    private static String lookup(String path, String template, String taskName, JsonNode tree) {
        if (path.isEmpty()) {
            throw new IllegalArgumentException(
                    "debounceKey \"" + template + "\" for task '" + taskName + "' has an empty {} placeholder");
        }
        JsonNode node = tree;
        for (String segment : path.split("\\.", -1)) {
            // Missing and explicit-null are the same mistake to the caller: the key they
            // asked for is not in this payload.
            node = node.isObject() ? node.get(segment) : null;
            if (node == null || node.isNull()) {
                throw new IllegalArgumentException("debounceKey \"" + template + "\" for task '" + taskName
                        + "' references {" + path + "}, which this payload does not provide");
            }
        }
        if (!node.isValueNode()) {
            // An object or array would stringify to something stable but meaningless,
            // collapsing distinct calls into one window rather than failing loudly.
            throw new IllegalArgumentException("debounceKey \"" + template + "\" for task '" + taskName
                    + "' references {" + path + "}, which is "
                    + node.getNodeType().toString().toLowerCase(java.util.Locale.ROOT)
                    + " — only scalar properties can key a debounce window");
        }
        return node.asText();
    }
}
