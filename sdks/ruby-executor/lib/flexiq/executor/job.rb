# frozen_string_literal: true

require "json"

module FlexiQ
  module Executor
    # One dispatched job, as a handler sees it.
    #
    # Cancellation and timeouts are cooperative: Ruby cannot stop a thread safely, so a long
    # handler calls `check!` between units of work, or reads `cancelled?` and `deadline` itself.
    class Job
      LOG_LEVELS = %w[info warn error result].freeze

      attr_reader :id, :task_name, :queue, :namespace, :retry_count, :max_retries, :timeout,
                  :disabled_middleware, :metadata, :payload, :lease, :deadline

      # `side` (an Outbox) receives progress and log frames; nil when the scheduler did not
      # acknowledge `side_channel`, which makes both calls no-ops.
      def initialize(frame, side: nil, now: Clock.now)
        @id = frame.id
        @task_name = frame.task_name
        @queue = frame.queue
        @namespace = frame.has_namespace? ? frame.namespace : nil
        @retry_count = frame.retry_count
        @max_retries = frame.max_retries
        @timeout = seconds(frame.timeout)
        @disabled_middleware = frame.disabled_middleware.to_a.freeze
        @metadata = frame.has_metadata? ? frame.metadata : nil
        @payload = frame.payload.b.freeze
        # Echoed on every frame about this attempt, never inspected or built here.
        @lease = frame.has_lease? && !frame.lease.empty? ? frame.lease.b.freeze : nil
        @deadline = @timeout ? now + @timeout : nil
        @side = side
        @cancel_requested = false
      end

      # Positional arguments, decoded from the payload on first use.
      def args = call[0]

      # Keyword arguments as a Hash with String keys, decoded from the payload on first use.
      def kwargs = call[1]

      def cancelled? = @cancel_requested

      def timed_out? = !@deadline.nil? && Clock.now >= @deadline

      # Raises Cancelled once the scheduler asked for this job to stop, TimeoutError once its
      # timeout has passed.
      def check!
        raise Cancelled, "job #{id} was cancelled" if cancelled?
        raise TimeoutError, "job #{id} timed out after #{(timeout * 1000).round}ms" if timed_out?
      end

      # Reports progress, clamped to 0..100. A no-op without the side channel.
      def progress(percent)
        return unless @side

        frame = V1::ProgressFrame.new(job_id: id, progress: percent.to_i.clamp(0, 100), lease: lease)
        @side.push_side(id, V1::AttachRequest.new(progress: frame), progress: true)
      end

      # Writes a task log line. `extra` is any JSON-encodable value.
      def log(level, message, extra: nil)
        level = level.to_s
        raise ArgumentError, "log level must be one of #{LOG_LEVELS.join(", ")}" unless LOG_LEVELS.include?(level)

        # Encoded before the side-channel check, so a bad value fails the same way either way.
        encoded = extra.nil? ? nil : JSON.generate(extra)
        return unless @side

        frame = V1::TaskLogFrame.new(job_id: id, task_name: task_name, level: level, message: message.to_s,
                                     extra: encoded&.b, lease: lease)
        @side.push_side(id, V1::AttachRequest.new(task_log: frame), progress: false)
      end

      # Publishes a partial result: a log line at level `result` whose value lives in `extra`.
      def publish(value) = log("result", "", extra: value)

      # Called by the session when a `cancel` frame names this job.
      def request_cancel! = (@cancel_requested = true)

      private

      def call = (@call ||= FlexiQ::Payload.decode_call(payload))

      def seconds(duration)
        return nil if duration.nil?

        value = duration.seconds + Rational(duration.nanos, 1_000_000_000)
        value.positive? ? value : nil
      end
    end
  end
end
