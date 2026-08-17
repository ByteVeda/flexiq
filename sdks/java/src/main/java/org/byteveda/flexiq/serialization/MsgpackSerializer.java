package org.byteveda.flexiq.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Type;
import org.byteveda.flexiq.errors.SerializationException;
import org.jspecify.annotations.Nullable;
import org.msgpack.jackson.dataformat.MessagePackFactory;

/**
 * A compact binary {@link Serializer} using MessagePack (via Jackson).
 *
 * <p>Optional: the {@code org.msgpack:jackson-dataformat-msgpack} dependency is
 * {@code compileOnly}, so a consumer that selects this serializer must add it to
 * their own build. JSON ({@link JsonSerializer}) remains the default.
 */
public final class MsgpackSerializer implements Serializer {
    private final ObjectMapper mapper;

    public MsgpackSerializer() {
        this(new ObjectMapper(new MessagePackFactory()));
    }

    public MsgpackSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(@Nullable Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new SerializationException("failed to serialize payload to MessagePack", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) {
        try {
            return mapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new SerializationException("failed to deserialize MessagePack payload", e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes, Type type) {
        try {
            return mapper.readValue(bytes, mapper.getTypeFactory().constructType(type));
        } catch (Exception e) {
            throw new SerializationException("failed to deserialize MessagePack payload", e);
        }
    }
}
