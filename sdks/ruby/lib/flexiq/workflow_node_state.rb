# frozen_string_literal: true

module FlexiQ
  # One node's state within a run. `status` is a FlexiQ::WorkflowNodeStatus symbol, or the raw
  # wire value. `job_id` is nil until the node's job exists; read the job with `Client#get_job`.
  WorkflowNodeState = Data.define(:name, :status, :job_id, :started_at, :completed_at, :error) do
    def self.from_json(json)
      new(
        name: json["name"], status: WorkflowNodeStatus.load(json["status"]), job_id: json["jobId"],
        started_at: Wire::Timestamp.load(json["startedAt"]),
        completed_at: Wire::Timestamp.load(json["completedAt"]), error: json["error"]
      )
    end

    def terminal? = WorkflowNodeStatus.terminal?(status)
  end
end
