# frozen_string_literal: true

module FlexiQ
  # One job as the server reports it.
  #
  # Timestamps are `Time` (UTC), durations are seconds as `Rational`, and `payload` / `result` are
  # the raw envelope bytes — nil unless requested on `Client#get_job` and present. `status` is a
  # Symbol from FlexiQ::JobStatus, or the raw wire value when this build does not know it.
  Job = Data.define(
    :id, :queue, :task_name, :status, :priority, :created_at, :scheduled_at,
    :retry_count, :max_retries, :timeout, :cancel_requested, :has_deps, :namespace,
    :started_at, :completed_at, :payload, :result, :error, :progress, :metadata, :notes,
    :unique_key, :expires_at, :result_ttl, :debounce_key, :enqueued_by
  ) do
    # Builds a Job from its proto3 JSON object; unknown keys are ignored as "not this build".
    def self.from_json(json)
      new(
        id: json["id"], queue: json["queue"], task_name: json["taskName"],
        status: JobStatus.load(json["status"]), priority: json["priority"] || 0,
        created_at: Wire::Timestamp.load(json["createdAt"]),
        scheduled_at: Wire::Timestamp.load(json["scheduledAt"]),
        retry_count: json["retryCount"] || 0, max_retries: json["maxRetries"] || 0,
        timeout: Wire::Duration.load(json["timeout"]),
        cancel_requested: json["cancelRequested"] == true, has_deps: json["hasDeps"] == true,
        namespace: json["namespace"],
        started_at: Wire::Timestamp.load(json["startedAt"]),
        completed_at: Wire::Timestamp.load(json["completedAt"]),
        payload: Wire::Bytes.load(json["payload"]), result: Wire::Bytes.load(json["result"]),
        error: json["error"], progress: json["progress"], metadata: json["metadata"], notes: json["notes"],
        unique_key: json["uniqueKey"], expires_at: Wire::Timestamp.load(json["expiresAt"]),
        result_ttl: Wire::Duration.load(json["resultTtl"]), debounce_key: json["debounceKey"],
        enqueued_by: json["enqueuedBy"]
      )
    end

    # Finished for good. An unrecognised status is never terminal.
    def terminal? = JobStatus.terminal?(status)

    def known_status? = JobStatus.known?(status)

    def cancel_requested? = cancel_requested

    # `[args, kwargs]` from the payload. Requires `get_job(..., include_payload: true)`.
    def decode_payload
      raise CodecError, "job #{id} carries no payload; request it with include_payload: true" if payload.nil?

      Payload.decode_call(payload)
    end

    # The value the task returned. Requires `get_job(..., include_result: true)`.
    def decode_result
      raise CodecError, "job #{id} carries no result; request it with include_result: true" if result.nil?

      Payload.decode_result(result)
    end

    # The failure as a TaskError, or nil when the job has not failed.
    def task_error = error && TaskError.parse(error)
  end
end
