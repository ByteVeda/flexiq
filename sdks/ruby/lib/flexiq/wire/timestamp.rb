# frozen_string_literal: true

require "time"

module FlexiQ
  module Wire
    # proto3 JSON `google.protobuf.Timestamp`: an RFC 3339 instant in UTC.
    module Timestamp
      module_function

      def dump(time)
        raise ArgumentError, "a timestamp must be a Time, got #{time.class}" unless time.is_a?(Time)

        time.getutc.iso8601(9)
      end

      # nil for an absent or unreadable value: a bad timestamp should not sink the whole job.
      def load(text)
        return nil if text.nil?

        Time.iso8601(text)
      rescue ArgumentError
        nil
      end
    end
  end
end
