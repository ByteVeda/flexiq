# frozen_string_literal: true

module FlexiQ
  # `WorkflowNodeStatus` as symbols. An unknown value stays raw and is never terminal.
  # A runnable node reads `:pending`: readiness is a predicate over the graph, not a status.
  module WorkflowNodeStatus
    BY_WIRE_NAME = {
      "WORKFLOW_NODE_STATUS_PENDING" => :pending,
      "WORKFLOW_NODE_STATUS_RUNNING" => :running,
      "WORKFLOW_NODE_STATUS_COMPLETED" => :completed,
      "WORKFLOW_NODE_STATUS_FAILED" => :failed,
      "WORKFLOW_NODE_STATUS_SKIPPED" => :skipped,
      "WORKFLOW_NODE_STATUS_WAITING_APPROVAL" => :waiting_approval,
      "WORKFLOW_NODE_STATUS_CACHE_HIT" => :cache_hit,
      "WORKFLOW_NODE_STATUS_COMPENSATING" => :compensating,
      "WORKFLOW_NODE_STATUS_COMPENSATED" => :compensated,
      "WORKFLOW_NODE_STATUS_COMPENSATION_FAILED" => :compensation_failed
    }.freeze

    KNOWN = BY_WIRE_NAME.values.freeze
    TERMINAL = %i[completed failed skipped cache_hit compensated compensation_failed].freeze

    module_function

    def load(value) = BY_WIRE_NAME.fetch(value, value)

    def known?(status) = KNOWN.include?(status)

    def terminal?(status) = TERMINAL.include?(status)
  end
end
