# frozen_string_literal: true

module FlexiQ
  module Wire
    # proto3 JSON `google.protobuf.Duration`: seconds with an `s` suffix, as in `"30s"` or `"1.500s"`.
    module Duration
      NANOS_PER_SECOND = 1_000_000_000
      PATTERN = /\A(-)?(\d+)(?:\.(\d{1,9}))?s\z/

      module_function

      # Seconds (Integer, Float or Rational) to the wire string; nanosecond precision.
      def dump(seconds)
        raise ArgumentError, "a duration must be Numeric, got #{seconds.class}" unless seconds.is_a?(Numeric)
        raise ArgumentError, "a duration must be finite" if seconds.is_a?(Float) && !seconds.finite?
        raise ArgumentError, "a duration cannot be negative: #{seconds}" if seconds.negative?

        whole, nanos = (seconds.to_r * NANOS_PER_SECOND).round.divmod(NANOS_PER_SECOND)
        "#{whole}#{fraction(nanos)}s"
      end

      # The wire string to seconds as a Rational, so no precision is lost; nil when unreadable.
      def load(text)
        match = PATTERN.match(text.to_s) or return nil

        magnitude = Integer(match[2], 10) + Rational(Integer((match[3] || "0").ljust(9, "0"), 10), NANOS_PER_SECOND)
        match[1] ? -magnitude : magnitude
      end

      # Same digit groups the server writes: none, millis, micros or nanos.
      def fraction(nanos)
        if nanos.zero? then ""
        elsif (nanos % 1_000_000).zero? then format(".%03d", nanos / 1_000_000)
        elsif (nanos % 1_000).zero? then format(".%06d", nanos / 1_000)
        else format(".%09d", nanos)
        end
      end
      private_class_method :fraction
    end
  end
end
