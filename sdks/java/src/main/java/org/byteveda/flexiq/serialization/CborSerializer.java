package org.byteveda.flexiq.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.byteveda.flexiq.errors.SerializationException;
import org.jspecify.annotations.Nullable;

/**
 * CBOR serializer for cross-SDK payloads (RFC 8949), writing the {@code 0x02}
 * wire-envelope tag from the binding contract. Use for tasks produced or
 * consumed by another FlexiQ SDK: unlike JSON, CBOR round-trips 64-bit and
 * larger integers, {@code byte[]}, and decimals losslessly across languages.
 *
 * <p>Call payloads use the cross-SDK call body {@code [args, kwargs]} with the
 * task payload as the single positional argument. Requires the optional
 * {@code com.fasterxml.jackson.dataformat:jackson-dataformat-cbor} dependency.
 */
public final class CborSerializer implements Serializer {
    private static final byte TAG_NATIVE = 0x00;
    private static final byte TAG_CBOR = 0x02;

    private final ObjectMapper mapper;

    public CborSerializer() {
        this(CBORMapper.builder().build());
    }

    public CborSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(@Nullable Object value) {
        try {
            return tagged(encode(value));
        } catch (Exception e) {
            throw new SerializationException("failed to serialize payload", e);
        }
    }

    /** Definite-length containers keep the bytes identical to peer SDKs — see the encoder. */
    private byte[] encode(@Nullable Object value) throws IOException {
        return DefiniteLengthCbor.encode(mapper, value);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) {
        try {
            return mapper.readValue(body(bytes), type);
        } catch (SerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new SerializationException("failed to deserialize payload", e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes, Type type) {
        try {
            return mapper.readValue(body(bytes), mapper.getTypeFactory().constructType(type));
        } catch (SerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new SerializationException("failed to deserialize payload", e);
        }
    }

    @Override
    public byte[] serializeCall(@Nullable Object payload) {
        List<Object> call = Arrays.asList(Arrays.asList(payload), Collections.emptyMap());
        try {
            return tagged(encode(call));
        } catch (Exception e) {
            throw new SerializationException("failed to serialize call payload", e);
        }
    }

    @Override
    public Object deserializeCall(byte[] bytes, Type payloadType) {
        try {
            JsonNode call = mapper.readTree(body(bytes));
            if (!call.isArray() || call.size() != 2 || !call.get(0).isArray()) {
                throw new SerializationException("CBOR call payload is not the [args, kwargs] wire shape");
            }
            JsonNode args = call.get(0);
            JsonNode first =
                    args.size() > 0 ? args.get(0) : mapper.getNodeFactory().nullNode();
            return mapper.convertValue(first, mapper.getTypeFactory().constructType(payloadType));
        } catch (SerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new SerializationException("failed to deserialize call payload", e);
        }
    }

    private static byte[] tagged(byte[] cborBody) {
        byte[] out = new byte[cborBody.length + 1];
        out[0] = TAG_CBOR;
        System.arraycopy(cborBody, 0, out, 1, cborBody.length);
        return out;
    }

    private static byte[] body(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new SerializationException("cannot deserialize an empty payload");
        }
        if (bytes[0] == TAG_CBOR) {
            return Arrays.copyOfRange(bytes, 1, bytes.length);
        }
        if (bytes[0] == TAG_NATIVE) {
            throw new SerializationException("payload is native-tagged (0x00): produced by a"
                    + " same-language-only serializer, not readable as CBOR wire format");
        }
        throw new SerializationException(
                String.format("payload is not CBOR wire format (tag 0x%02x, expected 0x02)", bytes[0]));
    }
}
