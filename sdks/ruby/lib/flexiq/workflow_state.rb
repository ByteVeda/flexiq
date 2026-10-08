# frozen_string_literal: true

module FlexiQ
  # `WorkflowState` as symbols. An unknown value stays raw and is never terminal: reading a
  # newer server's state as finished would stop a poller watching a run that is still going.
  module WorkflowState
    BY_WIRE_NAME = {
      "WORKFLOW_STATE_PENDING" => :pending,
      "WORKFLOW_STATE_RUNNING" => :running,
      "WORKFLOW_STATE_PAUSED" => :paused,
      "WORKFLOW_STATE_COMPLETED" => :completed,
      "WORKFLOW_STATE_COMPLETED_WITH_FAILURES" => :completed_with_failures,
      "WORKFLOW_STATE_FAILED" => :failed,
      "WORKFLOW_STATE_CANCELLED" => :cancelled,
      "WORKFLOW_STATE_COMPENSATING" => :compensating,
      "WORKFLOW_STATE_COMPENSATED" => :compensated,
      "WORKFLOW_STATE_COMPENSATION_FAILED" => :compensation_failed
    }.freeze

    KNOWN = BY_WIRE_NAME.values.freeze
    TERMINAL = %i[completed completed_with_failures failed cancelled compensated compensation_failed].freeze

    module_function

    def load(value) = BY_WIRE_NAME.fetch(value, value)

    def known?(state) = KNOWN.include?(state)

    def terminal?(state) = TERMINAL.include?(state)
  end
end
