# frozen_string_literal: true

module FlexiQ
  # One item of an EnqueueBatch: either `enqueued` (an EnqueueResult, durable) or `error`
  # (an unraised RPCError — that item alone did not land). A batch is never atomic.
  #
  # Both nil means a newer server answered with an outcome this build does not know.
  BatchItemResult = Data.define(:index, :enqueued, :error) do
    def self.from_json(json, index)
      new(
        index: index,
        enqueued: json["enqueued"] && EnqueueResult.from_json(json["enqueued"]),
        error: json["error"] && RPCError.from_status(json["error"])
      )
    end

    def enqueued? = !enqueued.nil?

    def failed? = !error.nil?
  end
end
