# frozen_string_literal: true

module FlexiQ
  module Executor
    # Raise from a handler to fail the job without a retry. Only the executor can see the
    # exception, so whether to retry is its decision, not the scheduler's.
    #
    # Raised with a cause (`raise Fatal, "…"` inside a `rescue`), the job's error names the cause.
    class Fatal < StandardError; end

    # Raised by Job#check! once the scheduler asked for the job to stop. A handler that lets it
    # propagate settles the job as cancelled rather than failed.
    class Cancelled < StandardError; end

    # Raised by Job#check! once the job's timeout has passed. The scheduler never tells an
    # attached executor about a timeout, so this is enforced here, cooperatively.
    class TimeoutError < StandardError; end

    # The scheduler refused the attach, or the stream failed.
    #
    # `permanent?` is true where reconnecting would only be refused again: a bad credential, a
    # missing scope, an executor id already attached, a protocol version mismatch.
    class AttachError < FlexiQ::Error
      PERMANENT_CODES = [
        GRPC::Core::StatusCodes::ALREADY_EXISTS,
        GRPC::Core::StatusCodes::FAILED_PRECONDITION,
        GRPC::Core::StatusCodes::UNAUTHENTICATED,
        GRPC::Core::StatusCodes::PERMISSION_DENIED
      ].freeze

      attr_reader :code, :details

      def initialize(code, details, permanent: PERMANENT_CODES.include?(code))
        @code = code
        @details = details
        @permanent = permanent
        super("flexiq: attach refused (code #{code}): #{details}")
      end

      def permanent? = @permanent

      def self.from(status) = new(status.code, status.details)
    end
  end
end
