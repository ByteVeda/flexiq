package org.byteveda.flexiq.serialization;

/**
 * A two-sided, byte-to-byte transform applied <em>after</em> serialization on the
 * producer and reversed <em>before</em> deserialization on the worker — for
 * compression, encryption, or signing. One implementation owns both directions
 * so the inverse cannot drift (cf. Temporal Payload Codec, Sidekiq middleware).
 *
 * <p>Codecs compose independently of the {@link Serializer}: a chain applies in
 * order on {@link #encode} and in reverse on {@link #decode}, over JSON or
 * MessagePack alike. Register them with {@code FlexiQ.builder().codec(...)}.
 */
public interface PayloadCodec {
    /**
     * Transform serialized bytes on the way out (producer).
     *
     * @param data the serialized payload, as the previous codec in the chain left it
     * @return the transformed bytes
     */
    byte[] encode(byte[] data);

    /**
     * Reverse {@link #encode} on the way in (worker).
     *
     * @param data the stored bytes, as the next codec in the chain left them
     * @return the bytes {@link #encode} was given
     */
    byte[] decode(byte[] data);
}
