# frozen_string_literal: true

module FlexiQ
  # A monotonic-clock deadline, immune to wall-clock steps.
  Deadline = Data.define(:at, :seconds) do
    # nil for nil (no limit).
    def self.within(seconds)
      return nil if seconds.nil?
      raise ArgumentError, "a timeout must be a positive number of seconds, got #{seconds.inspect}" unless
        seconds.is_a?(Numeric) && seconds.positive?

      new(at: now + seconds, seconds: seconds)
    end

    def self.now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def remaining = [at - self.class.now, 0].max

    def expired? = remaining.zero?

    # `limit`, capped at the time left.
    def cap(limit) = [limit, remaining].min
  end
end
