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
    /** Transform serialized bytes on the way out (producer). */
    byte[] encode(byte[] data);

    /** Reverse {@link #encode} on the way in (worker). */
    byte[] decode(byte[] data);
}
