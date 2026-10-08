# frozen_string_literal: true

module FlexiQ
  # A workflow run as the server reports it. `state` is a FlexiQ::WorkflowState symbol, or the
  # raw wire value when this build does not know it. Timestamps are `Time` (UTC) or nil.
  # `parent_run_id` / `parent_node_name` are set only on a sub-workflow's run.
  WorkflowRun = Data.define(
    :id, :definition_id, :state, :created_at, :started_at, :completed_at, :error,
    :parent_run_id, :parent_node_name
  ) do
    def self.from_json(json)
      new(
        id: json["id"], definition_id: json["definitionId"], state: WorkflowState.load(json["state"]),
        created_at: Wire::Timestamp.load(json["createdAt"]),
        started_at: Wire::Timestamp.load(json["startedAt"]),
        completed_at: Wire::Timestamp.load(json["completedAt"]),
        error: json["error"], parent_run_id: json["parentRunId"], parent_node_name: json["parentNodeName"]
      )
    end

    # Finished for good. An unrecognised state is never terminal.
    def terminal? = WorkflowState.terminal?(state)
  end
end
