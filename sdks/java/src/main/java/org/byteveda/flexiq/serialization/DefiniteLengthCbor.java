package org.byteveda.flexiq.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Encodes a value as CBOR whose maps and arrays carry a definite length.
 *
 * <p>Jackson streams values, so it does not know a container's size when it writes the header and
 * emits the indefinite-length form ({@code 0xbf ... 0xff}) where the cross-SDK wire contract pins
 * the definite-length one ({@code 0xa0}). Both decode identically, so payloads interoperated
 * either way — but the automatic {@code auto:} idempotency key hashes the serialized bytes, so the
 * divergence silently stopped idempotent enqueues deduping against peer SDKs. Every call payload
 * ends in the empty kwargs map, so it affected every payload rather than an edge case.
 *
 * <p>Databind builds the node tree first, which applies custom serializers, {@code @JsonInclude}
 * and the rest of the mapper's configuration; this class then walks that tree, where every size is
 * known, and writes the headers itself. Doing the sizing here rather than relying on a Jackson
 * version whose node serializers happen to pass a size keeps the wire format a property of this
 * code, not of a transitive dependency.
 *
 * <p>Only writing changes. Decoding still accepts both forms, so payloads written before this are
 * read back normally.
 */
final class DefiniteLengthCbor {

    /** Payloads are small; this only avoids the first few growth copies. */
    private static final int INITIAL_CAPACITY = 512;

    private DefiniteLengthCbor() {}

    /** The definite-length CBOR encoding of {@code value}, without the wire-envelope tag byte. */
    static byte[] encode(ObjectMapper mapper, @Nullable Object value) throws IOException {
        JsonNode tree = mapper.valueToTree(value);
        ByteArrayOutputStream out = new ByteArrayOutputStream(INITIAL_CAPACITY);
        try (JsonGenerator generator = mapper.getFactory().createGenerator(out)) {
            write(tree, generator);
        }
        return out.toByteArray();
    }

    /**
     * Writes one node, sizing containers as it goes. The default arm covers MISSING and POJO, which
     * have no CBOR form of their own, so databind decides how to render them.
     */
    private static void write(@Nullable JsonNode node, JsonGenerator generator) throws IOException {
        // `valueToTree(null)` returns null rather than a NullNode.
        if (node == null) {
            generator.writeNull();
            return;
        }
        switch (node.getNodeType()) {
            case OBJECT -> writeObject(node, generator);
            case ARRAY -> writeArray(node, generator);
            case STRING -> generator.writeString(node.textValue());
            case BOOLEAN -> generator.writeBoolean(node.booleanValue());
            case NULL -> generator.writeNull();
            case BINARY -> generator.writeBinary(node.binaryValue());
            case NUMBER -> writeNumber(node, generator);
            default -> generator.writeTree(node);
        }
    }

    private static void writeObject(JsonNode node, JsonGenerator generator) throws IOException {
        generator.writeStartObject(node, node.size());
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            generator.writeFieldName(field.getKey());
            write(field.getValue(), generator);
        }
        generator.writeEndObject();
    }

    private static void writeArray(JsonNode node, JsonGenerator generator) throws IOException {
        generator.writeStartArray(node, node.size());
        for (JsonNode item : node) {
            write(item, generator);
        }
        generator.writeEndArray();
    }

    /**
     * Writes each number in the width the wire contract pins — which is not the same rule for the
     * two kinds of number.
     *
     * <p>An integer keeps the narrowest representation the node already holds, because the contract
     * pins the shortest form that holds the value. A float goes the other way and is always widened
     * to 64 bits ({@code 0xfb}): a Java {@code float} would otherwise reach the wire as a 4-byte
     * CBOR float ({@code 0xfa}) where the same value sent as a {@code double} takes 8. Widening is
     * lossless — every {@code float} is exactly a {@code double} — and those bytes are what the
     * automatic {@code auto:} key hashes, so the narrower width would interoperate fine and
     * silently stop idempotent enqueues deduping across runtimes. A non-finite float is exempt from
     * the pinned width and lands wide here like any other.
     */
    private static void writeNumber(JsonNode node, JsonGenerator generator) throws IOException {
        if (node.isInt()) {
            generator.writeNumber(node.intValue());
        } else if (node.isLong()) {
            generator.writeNumber(node.longValue());
        } else if (node.isShort()) {
            generator.writeNumber(node.shortValue());
        } else if (node.isBigInteger()) {
            generator.writeNumber(node.bigIntegerValue());
        } else if (node.isFloat() || node.isDouble()) {
            generator.writeNumber(node.doubleValue());
        } else if (node.isBigDecimal()) {
            generator.writeNumber(node.decimalValue());
        } else {
            generator.writeTree(node);
        }
    }
}
