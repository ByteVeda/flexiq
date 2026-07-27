package org.byteveda.taskito.serialization;

import java.lang.reflect.Type;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Applies a chain of {@link PayloadCodec}s around a delegate {@link Serializer}:
 * each codec's {@link PayloadCodec#encode} runs in order after serialization, and
 * {@link PayloadCodec#decode} runs in reverse before deserialization. This keeps
 * the codec layer independent of the serializer (any chain works over JSON or
 * MessagePack) while reusing the single serializer channel the worker already
 * receives — so producer and worker apply the same transforms.
 */
public final class CodecSerializer implements Serializer {
    private final Serializer delegate;
    private final List<PayloadCodec> codecs;

    public CodecSerializer(Serializer delegate, List<PayloadCodec> codecs) {
        this.delegate = delegate;
        this.codecs = List.copyOf(codecs);
    }

    @Override
    public byte[] serialize(@Nullable Object value) {
        byte[] bytes = delegate.serialize(value);
        for (PayloadCodec codec : codecs) {
            bytes = codec.encode(bytes);
        }
        return bytes;
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) {
        return delegate.deserialize(decode(bytes), type);
    }

    @Override
    public Object deserialize(byte[] bytes, Type type) {
        return delegate.deserialize(decode(bytes), type);
    }

    /** Reverse the encode chain: last codec first. */
    private byte[] decode(byte[] bytes) {
        for (int i = codecs.size() - 1; i >= 0; i--) {
            bytes = codecs.get(i).decode(bytes);
        }
        return bytes;
    }
}
