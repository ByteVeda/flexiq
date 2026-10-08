# frozen_string_literal: true

module FlexiQ
  # A job's state, or a change to it. `kind`: TransitionKind; `status`: JobStatus; `attempt`:
  # retry count. `cursor` is nil on an id watch.
  JobTransition = Data.define(
    :job_id, :queue, :task_name, :kind, :status, :attempt, :time, :terminal,
    :error, :reason, :timed_out, :wake_at, :cursor
  ) do
    def self.from_json(json, cursor: nil)
      new(
        job_id: json["jobId"], queue: json["queue"], task_name: json["taskName"],
        kind: TransitionKind.load(json["kind"]), status: JobStatus.load(json["status"]),
        attempt: Wire::Int64.load(json["attempt"]) || 0, time: Wire::Timestamp.load(json["time"]),
        terminal: json["terminal"] == true, error: json["error"], reason: json["reason"],
        timed_out: json["timedOut"] == true, wake_at: Wire::Timestamp.load(json["wakeAt"]), cursor: cursor
      )
    end

    # The job is finished. Use this, never `status`: `:failed` may still retry or dead-letter.
    def terminal? = terminal

    def timed_out? = timed_out
  end
end
