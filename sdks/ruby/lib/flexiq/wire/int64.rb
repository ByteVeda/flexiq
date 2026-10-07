# frozen_string_literal: true

module FlexiQ
  module Wire
    # proto3 JSON writes 64-bit integers as strings, since a JSON number is a double.
    module Int64
      module_function

      def dump(value) = Integer(value).to_s

      # Accepts the string form and, leniently, a bare number; nil when unreadable.
      def load(value)
        case value
        when Integer then value
        when String then Integer(value, 10, exception: false)
        end
      end
    end
  end
end
