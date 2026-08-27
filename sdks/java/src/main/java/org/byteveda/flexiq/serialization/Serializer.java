package org.byteveda.flexiq.serialization;

import java.lang.reflect.Type;
import org.jspecify.annotations.Nullable;

/** Converts task payloads and results to and from the opaque bytes the core stores. */
public interface Serializer {
    /**
     * Encode a task payload or result.
     *
     * @param value what the caller passed, or {@code null} for a task with no payload
     * @return the bytes the core stores without interpreting them
     */
    byte[] serialize(@Nullable Object value);

    /**
     * Decode bytes this serializer produced.
     *
     * @param bytes what {@link #serialize} wrote
     * @param type the class to decode into
     * @param <T> that class
     * @return the decoded value
     */
    <T> T deserialize(byte[] bytes, Class<T> type);

    /**
     * Deserialize to a possibly-generic {@link Type} (from a
     * {@code TypeReference}). The default handles plain {@code Class} types and
     * rejects generic ones; a generics-aware serializer (e.g. {@link JsonSerializer})
     * overrides this.
     *
     * @param bytes what {@link #serialize} wrote
     * @param type the type to decode into, its type arguments preserved
     * @return the decoded value
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
     *
     * @param payload the task's argument, or {@code null} for a task that takes none
     * @return the bytes the job row carries
     */
    default byte[] serializeCall(@Nullable Object payload) {
        return serialize(payload);
    }

    /**
     * Inverse of {@link #serializeCall}: decode a call payload to the handler argument.
     *
     * @param bytes the job row's payload, possibly written by another SDK
     * @param payloadType the handler's argument type
     * @return the argument to hand the handler
     */
    default Object deserializeCall(byte[] bytes, Type payloadType) {
        return deserialize(bytes, payloadType);
    }
}
