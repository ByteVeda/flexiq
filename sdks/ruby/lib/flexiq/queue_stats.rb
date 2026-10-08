# frozen_string_literal: true

module FlexiQ
  # Job counts by state, for one queue or the whole namespace.
  QueueStats = Data.define(:pending, :running, :completed, :failed, :dead, :cancelled) do
    def self.from_json(json)
      new(**members.to_h { |name| [name, Wire::Int64.load(json[name.to_s]) || 0] })
    end

    def total = to_h.values.sum
  end
end
