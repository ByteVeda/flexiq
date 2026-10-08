# frozen_string_literal: true

module FlexiQ
  module Wire
    # proto3 JSON `bytes`: base64. Written standard and padded; read in either alphabet,
    # padding optional, as proto3 JSON allows.
    module Bytes
      module_function

      def dump(bytes) = [bytes].pack("m0")

      def load(text)
        return nil if text.nil?

        standard = text.tr("-_", "+/")
        standard += "=" * (-standard.length % 4)
        standard.unpack1("m0")
      rescue ArgumentError
        raise CodecError, "a bytes field is not valid base64"
      end
    end
  end
end
