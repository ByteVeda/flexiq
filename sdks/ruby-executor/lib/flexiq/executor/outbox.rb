# frozen_string_literal: true

module FlexiQ
  module Executor
    # Every frame bound for the attach stream, handed to gRPC through one Enumerator.
    #
    # grpc-ruby drains a bidi request Enumerator on a single thread of its own, so the stream has
    # exactly one writer. Two queues feed it:
    #
    # * settling frames (and `hello`) are never dropped;
    # * side-channel frames are bounded: a job's progress coalesces to its latest value, and past
    #   the limit the oldest frame is dropped. Telemetry must not hold back a result.
    #
    # A job's pending side-channel frames are promoted ahead of its settling frame: the scheduler
    # drops a log line that lands after the job it names has settled.
    class Outbox
      SIDE_LIMIT = 256
      # How often an idle writer checks whether the stream has ended under it.
      IDLE_POLL = 0.05

      Entry = Struct.new(:job_id, :progress, :request)
      private_constant :Entry

      attr_reader :dropped

      def initialize
        @mutex = Mutex.new
        @changed = ConditionVariable.new
        @settles = []
        @side = []
        @dropped = 0
        @closed = false
        @drained = false
      end

      def push(request)
        @mutex.synchronize do
          @settles << request
          @changed.broadcast
        end
      end

      def push_side(job_id, request, progress:)
        @mutex.synchronize do
          entry = progress && @side.find { |queued| queued.progress && queued.job_id == job_id }
          if entry
            entry.request = request
          else
            @side << Entry.new(job_id, progress, request)
            @dropped += 1 if @side.length > SIDE_LIMIT && @side.shift
          end
          @changed.broadcast
        end
      end

      # Moves a job's pending side-channel frames onto the settling queue, in order.
      def promote(job_id)
        @mutex.synchronize do
          taken, @side = @side.partition { |entry| entry.job_id == job_id }
          @settles.concat(taken.map(&:request))
        end
      end

      # No more frames will be pushed: the writer sends what is queued, then ends the request
      # stream, which half-closes the call.
      def close
        @mutex.synchronize do
          @closed = true
          @changed.broadcast
        end
      end

      # Waits until the writer has sent everything and ended, up to `budget` seconds.
      def await_drained(budget)
        deadline = Clock.now + budget
        @mutex.synchronize do
          until @drained
            remaining = deadline - Clock.now
            return false unless remaining.positive?

            @changed.wait(@mutex, remaining)
          end
          true
        end
      end

      # The request stream. `ended` is polled while idle: grpc-ruby does not finish reading a
      # call until its request Enumerator finishes, so a stream the scheduler ended (a rotation)
      # would otherwise wait on a writer with nothing to write.
      def frames(ended)
        Enumerator.new do |out|
          while (request = next_frame(ended))
            out << request
          end
        ensure
          mark_drained
        end
      end

      private

      def next_frame(ended)
        @mutex.synchronize do
          loop do
            return nil if ended.call

            request = @settles.shift || @side.shift&.request
            return request if request
            return nil if @closed

            @changed.wait(@mutex, IDLE_POLL)
          end
        end
      end

      def mark_drained
        @mutex.synchronize do
          @drained = true
          @changed.broadcast
        end
      end
    end
  end
end
