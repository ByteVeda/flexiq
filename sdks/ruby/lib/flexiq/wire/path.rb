# frozen_string_literal: true

module FlexiQ
  module Wire
    # Builds request paths from caller-supplied names.
    module Path
      UNRESERVED = /[^A-Za-z0-9\-._~]/

      module_function

      # Percent-encodes one path segment, so an id or queue name cannot add segments or a verb.
      def segment(value)
        text = value.to_s
        raise ArgumentError, "a path segment cannot be empty" if text.empty?

        text.b.gsub(UNRESERVED) { |char| format("%%%02X", char.ord) }
      end
    end
  end
end
