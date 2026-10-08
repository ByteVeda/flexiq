# frozen_string_literal: true

module FlexiQ
  module CBOR
    # A CBOR tag this client has no meaning for, kept so the value is not lost.
    Tagged = Data.define(:tag, :value)

    # RFC 8949 reader. Accepts every form the encoder is forbidden to write —
    # indefinite lengths, non-shortest integers, half and single floats — because a
    # payload written before those rules, or by another writer, still has to run.
    class Decoder
      # Nesting cap, so hostile input cannot exhaust the stack.
      MAX_DEPTH = 512
      BREAK = Object.new.freeze
      private_constant :BREAK

      # Decodes exactly one CBOR item; trailing bytes are an error.
      def self.decode(bytes)
        decoder = new(bytes)
        value = decoder.read_value(0)
        decoder.finish
        value
      end

      def initialize(bytes)
        @bytes = bytes.b
        @pos = 0
      end

      def finish
        return if @pos == @bytes.bytesize

        raise CodecError, "#{@bytes.bytesize - @pos} trailing byte(s) after the CBOR item"
      end

      # One complete item; a break code is only legal where an indefinite container reads it.
      def read_value(depth)
        value = read_item(depth)
        raise CodecError, "unexpected CBOR break code" if value.equal?(BREAK)

        value
      end

      private

      def read_item(depth)
        raise CodecError, "CBOR nesting deeper than #{MAX_DEPTH}" if depth > MAX_DEPTH

        initial = take_byte
        major = initial >> 5
        info = initial & 0x1F
        case major
        when 0 then read_argument(info)
        when 1 then -1 - read_argument(info)
        when 2 then read_string(info, Encoding::BINARY, depth)
        when 3 then read_string(info, Encoding::UTF_8, depth)
        when 4 then read_array(info, depth)
        when 5 then read_map(info, depth)
        when 6 then Tagged.new(tag: read_argument(info), value: read_value(depth + 1))
        else read_simple(info)
        end
      end

      def take_byte
        raise CodecError, "CBOR input ended early" if @pos >= @bytes.bytesize

        byte = @bytes.getbyte(@pos)
        @pos += 1
        byte
      end

      def take(count)
        raise CodecError, "CBOR input ended early" if @pos + count > @bytes.bytesize

        slice = @bytes.byteslice(@pos, count)
        @pos += count
        slice
      end

      def read_argument(info)
        case info
        when 0..23 then info
        when 24 then take(1).unpack1("C")
        when 25 then take(2).unpack1("n")
        when 26 then take(4).unpack1("N")
        when 27 then take(8).unpack1("Q>")
        else raise CodecError, "invalid CBOR argument encoding #{info}"
        end
      end

      def indefinite?(info) = info == 31

      def read_string(info, encoding, depth)
        raw = indefinite?(info) ? read_chunks(encoding, depth) : take(read_argument(info))
        text = raw.dup.force_encoding(encoding)
        raise CodecError, "CBOR text string is not valid UTF-8" unless text.valid_encoding?

        text
      end

      # An indefinite string is a run of definite chunks of the same major type.
      def read_chunks(encoding, depth)
        out = +"".b
        loop do
          chunk = read_item(depth + 1)
          break if chunk.equal?(BREAK)
          raise CodecError, "indefinite CBOR string holds a chunk of another type" unless chunk.is_a?(String)
          raise CodecError, "indefinite CBOR string mixes text and bytes" unless chunk.encoding == encoding

          out << chunk.b
        end
        out
      end

      def read_array(info, depth)
        return Array.new(read_argument(info)) { read_value(depth + 1) } unless indefinite?(info)

        items = []
        loop do
          item = read_item(depth + 1)
          break if item.equal?(BREAK)

          items << item
        end
        items
      end

      def read_map(info, depth)
        map = {}
        if indefinite?(info)
          loop do
            key = read_item(depth + 1)
            break if key.equal?(BREAK)

            map[key] = read_value(depth + 1)
          end
        else
          read_argument(info).times { map[read_value(depth + 1)] = read_value(depth + 1) }
        end
        map
      end

      def read_simple(info)
        case info
        when 20 then false
        when 21 then true
        when 22, 23 then nil
        when 25 then half_float(take(2).unpack1("n"))
        when 26 then take(4).unpack1("g")
        when 27 then take(8).unpack1("G")
        when 31 then BREAK
        else raise CodecError, "unsupported CBOR simple value #{info}"
        end
      end

      # IEEE 754 binary16 has no pack directive; RFC 8949 appendix D spells the decode.
      def half_float(bits)
        exponent = (bits >> 10) & 0x1F
        mantissa = bits & 0x3FF
        magnitude =
          if exponent.zero? then Math.ldexp(mantissa, -24)
          elsif exponent == 31 then mantissa.zero? ? Float::INFINITY : Float::NAN
          else Math.ldexp(mantissa + 1024, exponent - 25)
          end
        bits.anybits?(0x8000) ? -magnitude : magnitude
      end
    end
  end
end
