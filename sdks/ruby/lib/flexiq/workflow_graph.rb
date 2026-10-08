# frozen_string_literal: true

module FlexiQ
  # A workflow's shape: its nodes and the edges between them.
  #
  # `nodes` are WorkflowNode (or Hashes of its fields); `edges` are WorkflowEdge, `[from, to]`
  # pairs or Hashes. A node with no inbound edge is eligible as soon as the run starts.
  WorkflowGraph = Data.define(:nodes, :edges) do
    def initialize(nodes:, edges: [])
      raise ArgumentError, "nodes must be an Array, got #{nodes.class}" unless nodes.is_a?(Array)
      raise ArgumentError, "edges must be an Array, got #{edges.class}" unless edges.is_a?(Array)

      nodes = nodes.map { |node| node.is_a?(Hash) ? WorkflowNode.new(**node) : node }
      super(nodes: nodes.freeze, edges: edges.map { |edge| WorkflowEdge.coerce(edge) }.freeze)
    end

    # Refuses, before any call, a graph the server would refuse in terms of its own compilation:
    # an edge to an undeclared node fails there as "missing from step_metadata".
    # A nested sub-workflow graph is not checked; the server refuses the node before reading it.
    def validate!
      raise ArgumentError, "a workflow graph needs at least one node" if nodes.empty?

      declared = {}
      nodes.each do |node|
        raise ArgumentError, "a graph node must be a WorkflowNode, got #{node.class}" unless node.is_a?(WorkflowNode)
        raise ArgumentError, "node #{node.name.inspect} is declared twice" if declared.key?(node.name)

        declared[node.name] = true
      end
      edges.each do |edge|
        [edge.from, edge.to].each do |end_name|
          next if declared.key?(end_name)

          raise ArgumentError,
                "edge #{edge.from.inspect} -> #{edge.to.inspect} names undeclared node #{end_name.inspect}"
        end
      end
      self
    end

    # The proto3 JSON `WorkflowGraph`: the bare shape, then each node's configuration by name.
    def to_wire
      {
        "nodes" => nodes.map { |node| { "name" => node.name } },
        "edges" => edges.map(&:to_wire),
        "nodeConfigs" => nodes.map(&:to_wire)
      }
    end
  end
end
