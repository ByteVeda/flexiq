package org.byteveda.flexiq.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.errors.SerializationException;
import org.junit.jupiter.api.Test;

class CborSerializerTest {

    private final CborSerializer cbor = new CborSerializer();

    @Test
    void roundTripsScalar() {
        byte[] bytes = cbor.serialize(42);
        assertEquals(42, cbor.deserialize(bytes, Integer.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void roundTripsMap() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("count", 3);
        value.put("name", "flexiq");

        Map<String, Object> back = cbor.deserialize(cbor.serialize(value), Map.class);

        assertEquals(3, ((Number) back.get("count")).intValue());
        assertEquals("flexiq", back.get("name"));
    }

    @Test
    void writesWireTag() {
        assertEquals(0x02, cbor.serialize(List.of(1, 2))[0]);
        assertEquals(0x02, cbor.serializeCall("payload")[0]);
    }

    @Test
    void roundTripsLongAndBigInteger() {
        long big = (1L << 53) + 1;
        assertEquals(big, cbor.deserialize(cbor.serialize(big), Long.class));
        BigInteger huge = BigInteger.TWO.pow(80);
        assertEquals(huge, cbor.deserialize(cbor.serialize(huge), BigInteger.class));
    }

    @Test
    void encodesCallsAsArgsKwargsWireShape() {
        byte[] bytes = cbor.serializeCall("value");
        List<?> call = cbor.deserialize(bytes, List.class);
        assertEquals(2, call.size());
        assertEquals(List.of("value"), call.get(0));
        assertEquals(Map.of(), call.get(1));
    }

    @Test
    void deserializeCallReturnsSinglePayloadArgument() {
        byte[] bytes = cbor.serializeCall("hello");
        assertEquals("hello", cbor.deserializeCall(bytes, String.class));
    }

    @Test
    void deserializeCallHandlesNullPayload() {
        byte[] bytes = cbor.serializeCall(null);
        assertEquals(null, cbor.deserializeCall(bytes, String.class));
    }

    @Test
    void rejectsNativeTaggedPayload() {
        SerializationException error = assertThrows(
                SerializationException.class, () -> cbor.deserialize(new byte[] {0x00, (byte) 0x80}, Object.class));
        assertTrue(error.getMessage().contains("native-tagged"));
    }

    @Test
    void rejectsUntaggedPayload() {
        SerializationException error = assertThrows(
                SerializationException.class, () -> cbor.deserialize("{\"json\":true}".getBytes(), Object.class));
        assertTrue(error.getMessage().contains("not CBOR wire format"));
    }

    @Test
    void rejectsEmptyPayload() {
        assertThrows(SerializationException.class, () -> cbor.deserialize(new byte[0], Object.class));
    }

    @Test
    void rejectsNonCallShapeInDeserializeCall() {
        byte[] notACall = cbor.serialize(Map.of("not", "a call"));
        SerializationException error =
                assertThrows(SerializationException.class, () -> cbor.deserializeCall(notACall, String.class));
        assertTrue(error.getMessage().contains("wire shape"));
    }

    @Test
    void decodesBindingContractVector() {
        // BINDING_CONTRACT.md test vector for call f(1, "a"): [[1, "a"], {}]
        byte[] vector = {0x02, (byte) 0x82, (byte) 0x82, 0x01, 0x61, 0x61, (byte) 0xa0};
        List<?> call = cbor.deserialize(vector, List.class);
        assertEquals(List.of(1, "a"), call.get(0));
        assertEquals(Map.of(), call.get(1));
    }

    @Test
    void defaultSerializerCallShapeIsUnchanged() {
        Serializer json = new JsonSerializer();
        byte[] bytes = json.serializeCall("plain");
        assertEquals("plain", json.deserializeCall(bytes, String.class));
        assertEquals("plain", json.deserialize(bytes, String.class));
    }
}
