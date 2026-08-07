package org.byteveda.taskito.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The encoder behind {@link CborSerializer}. {@code WireVectorsTest} pins the bytes the cross-SDK
 * contract requires; these cover the shapes and mapper settings that contract does not reach.
 */
class DefiniteLengthCborTest {

    private final ObjectMapper mapper = CBORMapper.builder().build();

    private String encode(Object value) throws Exception {
        StringBuilder out = new StringBuilder();
        for (byte b : DefiniteLengthCbor.encode(mapper, value)) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }

    @Test
    void writesEmptyContainersWithDefiniteLength() throws Exception {
        assertEquals("a0", encode(Map.of()));
        assertEquals("80", encode(List.of()));
    }

    @Test
    void sizesNestedContainersIndependently() throws Exception {
        // a1 6161 82 a0 a1 6162 01 — {"a": [{}, {"b": 1}]}
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("a", List.of(Map.of(), Map.of("b", 1)));

        assertEquals("a16161" + "82" + "a0" + "a1616201", encode(value));
    }

    @Test
    void neverEmitsAnIndefiniteLengthHeader() throws Exception {
        Map<String, Object> deep = new LinkedHashMap<>();
        deep.put("list", List.of(Map.of("x", List.of(1, 2, 3))));
        deep.put("empty", Map.of());

        String encoded = encode(deep);

        assertFalse(encoded.contains("bf"), "indefinite-length map header in " + encoded);
        assertFalse(encoded.contains("9f"), "indefinite-length array header in " + encoded);
    }

    @Test
    void keepsNumericWidths() throws Exception {
        assertEquals("01", encode(1));
        assertEquals("1b0020000000000001", encode(9007199254740993L));
        assertEquals("fb3ff8000000000000", encode(1.5d));
        assertEquals("c249010000000000000001", encode(new BigInteger("18446744073709551617")));
        assertEquals("20", encode(-1));
    }

    @Test
    void preservesBinaryAndNull() throws Exception {
        assertEquals("420102", encode(new byte[] {1, 2}));
        assertEquals("f6", encode(null));
    }

    /** Sizes must come from what databind actually emits, not from the source collection. */
    @Test
    void sizesReflectMapperInclusionRules() throws Exception {
        ObjectMapper skipsNulls = CBORMapper.builder()
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .build();
        Map<String, Object> withNull = new LinkedHashMap<>();
        withNull.put("kept", 1);
        withNull.put("dropped", null);

        StringBuilder out = new StringBuilder();
        for (byte b : DefiniteLengthCbor.encode(skipsNulls, withNull)) {
            out.append(String.format("%02x", b));
        }

        // a1, not a2: the suppressed entry must not be counted.
        assertEquals("a1646b65707401", out.toString());
    }

    @Test
    void encodesBigDecimalThroughTheNumberPath() throws Exception {
        assertFalse(encode(new BigDecimal("1.25")).isEmpty());
        assertEquals(
                new BigDecimal("1.25"),
                mapper.readValue(DefiniteLengthCbor.encode(mapper, new BigDecimal("1.25")), BigDecimal.class));
    }
}
