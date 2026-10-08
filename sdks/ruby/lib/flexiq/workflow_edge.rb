# frozen_string_literal: true

module FlexiQ
  # One edge of a workflow graph: `from` must finish before `to` becomes eligible.
  WorkflowEdge = Data.define(:from, :to) do
    # An edge from a WorkflowEdge, a `[from, to]` pair or a Hash of its fields.
    def self.coerce(edge)
      case edge
      when WorkflowEdge then edge
      when Hash then new(**edge)
      when Array
        raise ArgumentError, "an edge pair must have two names, got #{edge.length}" unless edge.length == 2

        new(from: edge[0], to: edge[1])
      else raise ArgumentError, "an edge must be a WorkflowEdge, a [from, to] pair or a Hash, got #{edge.class}"
      end
    end

    def to_wire = { "from" => from, "to" => to }
  end
end
