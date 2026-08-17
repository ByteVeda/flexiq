package org.byteveda.flexiq.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Type;
import org.byteveda.flexiq.errors.SerializationException;
import org.jspecify.annotations.Nullable;

/** Default {@link Serializer}: JSON via Jackson. */
public final class JsonSerializer implements Serializer {
    private final ObjectMapper mapper;

    public JsonSerializer() {
        this(new ObjectMapper());
    }

    public JsonSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(@Nullable Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new SerializationException("failed to serialize payload", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) {
        try {
            return mapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new SerializationException("failed to deserialize payload", e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes, Type type) {
        try {
            return mapper.readValue(bytes, mapper.getTypeFactory().constructType(type));
        } catch (Exception e) {
            throw new SerializationException("failed to deserialize payload", e);
        }
    }
}
