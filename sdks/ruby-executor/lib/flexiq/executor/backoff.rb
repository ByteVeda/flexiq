# frozen_string_literal: true

module FlexiQ
  module Executor
    # Reconnect delay: `min` doubling to `max`, with full jitter so a fleet the scheduler dropped
    # together does not return together. `reset` after an attach succeeds.
    class Backoff
      def initialize(min, max, random: Random.new)
        @min = min.to_f
        @max = max.to_f
        @random = random
        reset
      end

      def reset
        @ceiling = @min
      end

      def next_delay
        delay = @min + (@random.rand * (@ceiling - @min))
        @ceiling = [@ceiling * 2, @max].min
        delay
      end
    end
  end
end
