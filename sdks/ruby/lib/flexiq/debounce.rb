# frozen_string_literal: true

module FlexiQ
  # Collapses a burst of enqueues sharing `key` into one job. Durations are seconds.
  Debounce = Data.define(:key, :window, :max_wait, :replace_payload, :max_pending) do
    def initialize(key:, window: nil, max_wait: nil, replace_payload: false, max_pending: nil)
      super
    end

    def to_wire
      {
        "key" => key,
        "window" => window && Wire::Duration.dump(window),
        "maxWait" => max_wait && Wire::Duration.dump(max_wait),
        "replacePayload" => replace_payload || nil,
        "maxPending" => max_pending && Wire::Int64.dump(max_pending)
      }.compact
    end
  end
end
