# frozen_string_literal: true

module FlexiQ
  module Executor
    # The executor's concurrency: how many jobs may run at once, shared across reconnects so a
    # job still running from the last stream keeps its slot.
    class Slots
      attr_reader :total

      def initialize(total)
        @total = total
        @free = total
        @draining = false
        @mutex = Mutex.new
        @changed = ConditionVariable.new
      end

      # Takes a slot; false when none is free or the session is draining.
      def acquire
        @mutex.synchronize do
          return false if @draining || @free <= 0

          @free -= 1
          true
        end
      end

      def release
        @mutex.synchronize do
          @free = [@free + 1, total].min
          @changed.broadcast
        end
      end

      # Free capacity as a heartbeat reports it: zero while draining, so no new work is matched.
      def available = @mutex.synchronize { @draining ? 0 : @free }

      def in_flight = @mutex.synchronize { total - @free }

      def drain = @mutex.synchronize { @draining = true }

      def resume = @mutex.synchronize { @draining = false }

      # Waits for every running job to finish, up to `budget` seconds.
      def await_idle(budget)
        deadline = Clock.now + budget
        @mutex.synchronize do
          until @free >= total
            remaining = deadline - Clock.now
            return false unless remaining.positive?

            @changed.wait(@mutex, remaining)
          end
          true
        end
      end
    end
  end
end
