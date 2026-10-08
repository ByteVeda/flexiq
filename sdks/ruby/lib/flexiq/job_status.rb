# frozen_string_literal: true

module FlexiQ
  # `JobStatus` as Ruby symbols.
  #
  # A value this build does not recognise is kept as the server sent it (a String name, or an
  # Integer) and is never terminal: a newer server may grow a state, and reading it as finished
  # would have a poller stop watching a job that is still running.
  module JobStatus
    BY_WIRE_NAME = {
      "JOB_STATUS_PENDING" => :pending,
      "JOB_STATUS_RUNNING" => :running,
      "JOB_STATUS_COMPLETE" => :complete,
      "JOB_STATUS_FAILED" => :failed,
      "JOB_STATUS_DEAD" => :dead,
      "JOB_STATUS_CANCELLED" => :cancelled
    }.freeze

    KNOWN = BY_WIRE_NAME.values.freeze
    TERMINAL = %i[complete dead cancelled].freeze
    WIRE_NAME = BY_WIRE_NAME.invert.freeze

    module_function

    def load(value) = BY_WIRE_NAME.fetch(value, value)

    # The wire name for a request filter. A String passes as-is, so a state this build does not
    # know can still be named; an unknown Symbol is a typo and is refused before the call.
    def dump(status)
      case status
      when String then status
      when Symbol then WIRE_NAME.fetch(status) { raise ArgumentError, "unknown job status #{status.inspect}" }
      else raise ArgumentError, "a job status must be a Symbol or a String, got #{status.class}"
      end
    end

    def known?(status) = KNOWN.include?(status)

    def terminal?(status) = TERMINAL.include?(status)
  end
end
