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

    module_function

    def load(value) = BY_WIRE_NAME.fetch(value, value)

    def known?(status) = KNOWN.include?(status)

    def terminal?(status) = TERMINAL.include?(status)
  end
end
