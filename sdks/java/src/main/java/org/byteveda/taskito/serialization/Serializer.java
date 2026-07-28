package org.byteveda.taskito.serialization;

import java.lang.reflect.Type;
import org.jspecify.annotations.Nullable;

/** Converts task payloads and results to and from the opaque bytes the core stores. */
public interface Serializer {
    byte[] serialize(@Nullable Object value);

    <T> T deserialize(byte[] bytes, Class<T> type);

    /**
     * Deserialize to a possibly-generic {@link Type} (from a
     * {@code TypeReference}). The default handles plain {@code Class} types and
     * rejects generic ones; a generics-aware serializer (e.g. {@link JsonSerializer})
     * overrides this.
     */
    default Object deserialize(byte[] bytes, Type type) {
        if (type instanceof Class) {
            return deserialize(bytes, (Class<?>) type);
        }
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " does not support the generic payload type " + type
                + "; use a Jackson-based serializer or a non-generic Task payload type");
    }

    /**
     * Call-shaped encoding for task payloads. Wire serializers (e.g.
     * {@link CborSerializer}) override this to write the cross-SDK call body
     * {@code [args, kwargs]} from the binding contract; the default keeps the
     * bare-value body. Results always use {@link #serialize}/{@link #deserialize}.
     */
    default byte[] serializeCall(@Nullable Object payload) {
        return serialize(payload);
    }

    /** Inverse of {@link #serializeCall}: decode a call payload to the handler argument. */
    default Object deserializeCall(byte[] bytes, Type payloadType) {
        return deserialize(bytes, payloadType);
    }
}
