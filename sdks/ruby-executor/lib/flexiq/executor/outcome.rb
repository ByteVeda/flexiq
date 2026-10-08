# frozen_string_literal: true

module FlexiQ
  module Executor
    # How a handler ended, and the one settling frame that reports it.
    #
    # `value` is what the handler returned and `error` what it raised; exactly one is meaningful.
    # `timed_out` and `cancelled` are read off the job when the handler ends.
    Outcome = Data.define(:value, :error, :wall, :timed_out, :cancelled) do
      # The settling frame, with the job's lease echoed when the dispatch carried one.
      def frame(job)
        return success(job) if error.nil?
        return Outcome.timeout_failure(job, wall) if timed_out
        return cancelled_frame(job) if cancelled && error.is_a?(Cancelled)

        errtype, message, traceback = Outcome.describe(error)
        Outcome.failure(job, TaskError.encode(errtype, message, traceback), should_retry: !error.is_a?(Fatal),
                                                                            wall: wall)
      end

      # The error text core writes for a timeout it reaps itself, so both read alike.
      def self.timeout_failure(job, wall)
        message = "job timed out after #{(job.timeout * 1000).round}ms"
        failure(job, TaskError.encode("TimeoutError", message), should_retry: true, wall: wall, timed_out: true)
      end

      def self.failure(job, error_json, should_retry:, wall: nil, timed_out: false)
        V1::AttachRequest.new(failure: V1::FailureFrame.new(
          job_id: job.id, task_name: job.task_name, error: error_json,
          retry_count: job.retry_count, max_retries: job.max_retries,
          wall_time: wall && duration(wall), should_retry: should_retry, timed_out: timed_out, lease: job.lease
        ))
      end

      # A Fatal raised with a cause reports the cause: Fatal is the retry decision, not the error.
      def self.describe(error)
        error = error.cause if error.is_a?(Fatal) && error.cause
        [error.class.name || "Error", error.message.to_s, error.backtrace || []]
      end

      def self.duration(seconds)
        whole = seconds.floor
        Google::Protobuf::Duration.new(seconds: whole, nanos: ((seconds - whole) * 1_000_000_000).round)
      end

      private

      def success(job)
        result = value.nil? ? nil : FlexiQ::Payload.encode_result(value)
        V1::AttachRequest.new(success: V1::SuccessFrame.new(
          job_id: job.id, task_name: job.task_name, result: result,
          wall_time: Outcome.duration(wall), lease: job.lease
        ))
      rescue CodecError => e
        # The handler's side effects already happened; a retry would repeat them and fail the
        # same way, so this is fatal.
        message = "task #{job.task_name.inspect} returned a value that does not encode: #{e.message}"
        Outcome.failure(job, TaskError.encode("ResultEncodeError", message), should_retry: false, wall: wall)
      end

      def cancelled_frame(job)
        V1::AttachRequest.new(cancelled: V1::CancelledFrame.new(
          job_id: job.id, task_name: job.task_name, wall_time: Outcome.duration(wall), lease: job.lease
        ))
      end
    end
  end
end
