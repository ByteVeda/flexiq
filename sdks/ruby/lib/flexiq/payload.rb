# frozen_string_literal: true

module FlexiQ
  # The payload envelope: one codec tag byte, then that codec's body.
  #
  # A call body (`Job#payload`) is a two-element CBOR array `[args, kwargs]`.
  # A result body (`Job#result`) is a bare CBOR value with no wrapper.
  module Payload
    NATIVE = 0x00
    MSGPACK = 0x01
    CBOR_TAG = 0x02

    CODEC_NAMES = { NATIVE => "language-native", MSGPACK => "MessagePack" }.freeze
    private_constant :CODEC_NAMES

    module_function

    # Encodes a call as `0x02 ++ CBOR([args, kwargs])`.
    def encode_call(args = [], kwargs = {})
      raise ArgumentError, "args must be an Array, got #{args.class}" unless args.is_a?(Array)
      raise ArgumentError, "kwargs must be a Hash, got #{kwargs.class}" unless kwargs.is_a?(Hash)

      CBOR_TAG.chr.b << CBOR::Encoder.encode([args, kwargs])
    end

    # Decodes a call body into `[args, kwargs]`.
    def decode_call(bytes)
      body = decode(bytes)
      unless body.is_a?(Array) && body.length == 2 && body[0].is_a?(Array) && body[1].is_a?(Hash)
        raise CodecError, "a call body must be a two-element [args, kwargs] array"
      end

      body
    end

    # Decodes a result body into the value the task returned.
    def decode_result(bytes) = decode(bytes)

    # Strips the tag and decodes the CBOR body. Untagged payloads are never sniffed:
    # a raw CBOR or MessagePack body can begin with any byte.
    def decode(bytes)
      raise CodecError, "an empty payload carries no codec tag" if bytes.nil? || bytes.empty?

      tag = bytes.getbyte(0)
      return CBOR::Decoder.decode(bytes.byteslice(1..)) if tag == CBOR_TAG

      name = CODEC_NAMES.fetch(tag, "reserved")
      raise CodecError, format("payload codec tag 0x%<tag>02x (%<name>s) is not readable by this client",
                               tag: tag, name: name)
    end
  end
end
