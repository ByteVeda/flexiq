# frozen_string_literal: true

module FlexiQ
  module Executor
    # Monotonic seconds: deadlines and wall times must not move with the system clock.
    module Clock
      def self.now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
