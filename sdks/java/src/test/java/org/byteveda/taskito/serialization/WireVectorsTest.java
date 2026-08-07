package org.byteveda.taskito.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Asserts the shared cross-SDK wire vectors.
 *
 * <p>{@code contracts/wire-vectors.json} pins the bytes of the CBOR call
 * envelope. Every SDK runs this same file against its own serializer, so an
 * encoding change fails the runtime that made it instead of quietly producing
 * payloads its peers cannot read.
 */
class WireVectorsTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonNode VECTORS = load();

    private final CborSerializer cbor = new CborSerializer();

    /** Walk up to the repository root rather than counting directories. */
    private static JsonNode load() {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("contracts").resolve("wire-vectors.json");
            if (Files.isRegularFile(candidate)) {
                try {
                    return JSON.readTree(candidate.toFile());
                } catch (IOException e) {
                    throw new IllegalStateException("failed to read " + candidate, e);
                }
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("contracts/wire-vectors.json not found above the working directory");
    }

    private static List<JsonNode> cases(String section) {
        List<JsonNode> out = new ArrayList<>();
        VECTORS.get(section).forEach(out::add);
        return out;
    }

    /**
     * Java has no keyword arguments and {@code enqueue} takes a single payload,
     * so the serializer can only produce a one-positional-argument call with an
     * empty kwargs map. Every other case is still decoded below — a producer in
     * another runtime can enqueue one.
     */
    static Stream<JsonNode> encodable() {
        return cases("encode").stream()
                .filter(c -> c.get("kwargs").isEmpty())
                .filter(c -> c.get("args").size() == 1);
    }

    static Stream<JsonNode> encodeCases() {
        return cases("encode").stream();
    }

    static Stream<JsonNode> decodeOnlyCases() {
        return cases("decode_only").stream();
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            out.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return out.toString();
    }

    private static byte[] unhex(String text) {
        byte[] out = new byte[text.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(text.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    @ParameterizedTest(name = "encodes {0}")
    @MethodSource("encodable")
    void encodesToThePinnedBytes(JsonNode testCase) {
        Object payload = JSON.convertValue(testCase.get("args").get(0), Object.class);

        assertEquals(testCase.get("hex").asText(), hex(cbor.serializeCall(payload)));
    }

    /**
     * Guards the encoding choice that makes those bytes match in the first place.
     *
     * <p>Jackson streams values, so left alone it writes CBOR maps with indefinite
     * length ({@code 0xbf ... 0xff}) where this contract pins definite length
     * ({@code 0xa0}). Both decode identically, so a regression here would not break
     * interoperability — it would silently break the cross-SDK guarantee documented
     * on {@code IdempotencyKeys}, because the automatic {@code auto:} key hashes the
     * serialized payload. Every call ends in the empty kwargs map, so a regression
     * would shift <em>every</em> payload's key at once.
     */
    @Test
    void writesDefiniteLengthContainers() {
        String encoded = hex(cbor.serializeCall("x"));

        assertEquals("0282816178a0", encoded, "kwargs must be a definite-length empty map (a0), not bf..ff");
        assertTrue(!encoded.contains("bf"), "no indefinite-length container header may appear: " + encoded);
    }

    /** Payloads written before the definite-length fix must still decode. */
    @Test
    void decodesLegacyIndefiniteLengthPayloads() {
        // 02 82 81 a1 6161 6162 bfff — args definite, kwargs the old indefinite empty map.
        Object call = cbor.deserialize(unhex("028281a161616162bfff"), Object.class);

        assertEquals("[[{a=b}], {}]", String.valueOf(call));
    }

    @ParameterizedTest(name = "decodes {0}")
    @MethodSource("encodeCases")
    void decodesThePinnedBytes(JsonNode testCase) {
        byte[] raw = unhex(testCase.get("hex").asText());
        JsonNode call = JSON.valueToTree(cbor.deserialize(raw, Object.class));

        assertEquals(testCase.get("args"), call.get(0));
        assertEquals(testCase.get("kwargs"), call.get(1));
    }

    @ParameterizedTest(name = "decodes {0}")
    @MethodSource("decodeOnlyCases")
    void decodeOnlyVectors(JsonNode testCase) {
        byte[] raw = unhex(testCase.get("hex").asText());
        Object call = cbor.deserialize(raw, Object.class);

        if (testCase.path("round_trip_only").asBoolean()) {
            // The value has no lossless JSON form, so re-encoding is the assertion.
            List<?> parts = (List<?>) call;
            assertTrue(((java.util.Map<?, ?>) parts.get(1)).isEmpty());
            Object payload = ((List<?>) parts.get(0)).get(0);

            assertEquals(testCase.get("hex").asText(), hex(cbor.serializeCall(payload)));
        } else {
            JsonNode decoded = JSON.valueToTree(call);
            assertEquals(testCase.get("args"), decoded.get(0));
            assertEquals(testCase.get("kwargs"), decoded.get(1));
        }
    }

    @Test
    void keepsTheVectorQuotedInTheBindingContract() {
        JsonNode documented = cases("encode").stream()
                .filter(c -> "contract-vector".equals(c.get("name").asText()))
                .findFirst()
                .orElseThrow();
        assertEquals("028282016161a0", documented.get("hex").asText());
    }
}
