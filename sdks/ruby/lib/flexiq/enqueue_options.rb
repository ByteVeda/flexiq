# frozen_string_literal: true

module FlexiQ
  # Per-job settings for an enqueue. Every field is optional; an unset one takes the server's
  # default. Times are `Time`, durations are seconds (Integer, Float or Rational).
  #
  # `unique_key` dedupes against the *active* job only — once it finishes the key is free again —
  # so it is the value to reuse when resending a write whose outcome is unknown.
  EnqueueOptions = Data.define(
    :queue, :priority, :max_retries, :scheduled_at, :timeout, :unique_key, :metadata,
    :notes, :depends_on, :expires_at, :result_ttl, :debounce
  ) do
    def initialize(queue: nil, priority: nil, max_retries: nil, scheduled_at: nil, timeout: nil,
                   unique_key: nil, metadata: nil, notes: nil, depends_on: [], expires_at: nil,
                   result_ttl: nil, debounce: nil)
      debounce = Debounce.new(**debounce) if debounce.is_a?(Hash)
      super
    end

    # The proto3 JSON `EnqueueOptions` object, unset fields omitted.
    def to_wire
      {
        "queue" => queue,
        "priority" => priority,
        "maxRetries" => max_retries,
        "scheduledAt" => scheduled_at && Wire::Timestamp.dump(scheduled_at),
        "timeout" => timeout && Wire::Duration.dump(timeout),
        "uniqueKey" => unique_key,
        "metadata" => metadata,
        "notes" => notes,
        "dependsOn" => depends_on.empty? ? nil : depends_on.map(&:to_s),
        "expiresAt" => expires_at && Wire::Timestamp.dump(expires_at),
        "resultTtl" => result_ttl && Wire::Duration.dump(result_ttl),
        "debounce" => debounce&.to_wire
      }.compact
    end
  end
end
