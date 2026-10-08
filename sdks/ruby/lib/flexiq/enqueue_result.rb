# frozen_string_literal: true

module FlexiQ
  # What an accepted enqueue returned. `deduplicated` is true when a live job already held the
  # `unique_key` and that job came back instead of a new one.
  EnqueueResult = Data.define(:job, :deduplicated) do
    def self.from_json(json)
      new(job: json["job"] && Job.from_json(json["job"]), deduplicated: json["deduplicated"] == true)
    end

    alias_method :deduplicated?, :deduplicated
  end
end
