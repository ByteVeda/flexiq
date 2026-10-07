# frozen_string_literal: true

module FlexiQ
  module CBOR
    # RFC 8949 writer held to the three rules the wire contract pins:
    # definite-length containers, shortest-form integers, 64-bit finite floats.
    #
    # Each rule moves bytes without moving meaning, and the `auto:` idempotency key
    # hashes those bytes — so a drift here silently stops cross-runtime dedupe.
    module Encoder
      UINT64_MAX = (2**64) - 1
      NEGATIVE_MIN = -(2**64)

      # Preferred serialization for the non-finite floats; the contract leaves their width free.
      POSITIVE_INFINITY = "\xF9\x7C\x00".b.freeze
      NEGATIVE_INFINITY = "\xF9\xFC\x00".b.freeze
      NAN = "\xF9\x7E\x00".b.freeze

      module_function

      # Encodes one Ruby value into a binary String.
      #
      # A String tagged `Encoding::BINARY` is a CBOR byte string; any other String is text.
      # Symbols are text. Hash keys keep insertion order: the vectors pin unsorted keys.
      def encode(value)
        write(+"".b, value)
      end

      def write(out, value)
        case value
        when nil then out << "\xF6".b
        when true then out << "\xF5".b
        when false then out << "\xF4".b
        when Integer then write_integer(out, value)
        when Float then write_float(out, value)
        when Symbol then write_text(out, value.name)
        when String then write_string(out, value)
        when Array then write_array(out, value)
        when Hash then write_map(out, value)
        else raise CodecError, "cannot encode #{value.class} as CBOR"
        end
      end

      def write_integer(out, value)
        unless value.between?(NEGATIVE_MIN, UINT64_MAX)
          raise CodecError, "integer #{value} is outside CBOR's 64-bit range"
        end

        value.negative? ? write_head(out, 1, -1 - value) : write_head(out, 0, value)
      end

      def write_float(out, value)
        return out << NAN if value.nan?
        return out << (value.positive? ? POSITIVE_INFINITY : NEGATIVE_INFINITY) if value.infinite?

        out << "\xFB".b << [value].pack("G")
      end

      def write_string(out, value)
        return write_bytes(out, value) if value.encoding == Encoding::BINARY

        write_text(out, value)
      end

      def write_bytes(out, value)
        write_head(out, 2, value.bytesize)
        out << value
      end

      def write_text(out, value)
        text = value.encoding == Encoding::UTF_8 ? value : value.encode(Encoding::UTF_8)
        raise CodecError, "text is not valid UTF-8: #{text.inspect}" unless text.valid_encoding?

        write_head(out, 3, text.bytesize)
        out << text.b
      rescue EncodingError => e
        raise CodecError, "text cannot be converted to UTF-8: #{e.message}"
      end

      def write_array(out, value)
        write_head(out, 4, value.length)
        value.each { |item| write(out, item) }
        out
      end

      def write_map(out, value)
        write_head(out, 5, value.length)
        value.each do |key, item|
          write(out, key)
          write(out, item)
        end
        out
      end

      # Major type in the top three bits, then the argument in its shortest form.
      def write_head(out, major, argument)
        prefix = major << 5
        if argument < 24
          out << (prefix | argument).chr
        elsif argument <= 0xFF
          out << (prefix | 24).chr << [argument].pack("C")
        elsif argument <= 0xFFFF
          out << (prefix | 25).chr << [argument].pack("n")
        elsif argument <= 0xFFFF_FFFF
          out << (prefix | 26).chr << [argument].pack("N")
        else
          out << (prefix | 27).chr << [argument].pack("Q>")
        end
      end
    end
  end
end
