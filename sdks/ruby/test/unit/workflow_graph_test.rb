# frozen_string_literal: true

require_relative "../test_helper"

class WorkflowGraphTest < Minitest::Test
  def two_nodes
    FlexiQ::WorkflowGraph.new(
      nodes: [
        FlexiQ::WorkflowNode.new(name: "charge", task_name: "orders.charge", args: [{ "id" => "o-1" }],
                                 queue: "payments", max_retries: 2, priority: 5),
        { name: "ship", task_name: "orders.ship", kwargs: { "fast" => true }, timeout: 1.5,
          condition: :on_success, compensate: "orders.unship" }
      ],
      edges: [%w[charge ship]]
    )
  end

  def test_a_graph_splits_into_shape_and_named_configuration
    wire = two_nodes.to_wire

    assert_equal [{ "name" => "charge" }, { "name" => "ship" }], wire["nodes"]
    assert_equal [{ "from" => "charge", "to" => "ship" }], wire["edges"]
    assert_equal({ "name" => "charge", "taskName" => "orders.charge", "queue" => "payments",
                   "raw" => FlexiQ::Wire::Bytes.dump(FlexiQ::Payload.encode_call([{ "id" => "o-1" }], {})),
                   "maxRetries" => 2, "priority" => 5 }, wire["nodeConfigs"][0])
    assert_equal({ "name" => "ship", "taskName" => "orders.ship",
                   "raw" => FlexiQ::Wire::Bytes.dump(FlexiQ::Payload.encode_call([], { "fast" => true })),
                   "timeout" => "1.500s", "condition" => "EDGE_CONDITION_ON_SUCCESS",
                   "compensate" => "orders.unship" }, wire["nodeConfigs"][1])
  end

  def test_a_node_with_no_arguments_still_sends_the_empty_call
    wire = FlexiQ::WorkflowNode.new(name: "n", task_name: "t").to_wire

    assert_equal FlexiQ::Payload.encode_call([], {}), FlexiQ::Wire::Bytes.load(wire["raw"])
  end

  def test_edges_coerce_from_pairs_hashes_and_values
    edges = FlexiQ::WorkflowGraph.new(
      nodes: [], edges: [%w[a b], { from: "b", to: "c" }, FlexiQ::WorkflowEdge.new(from: "c", to: "d")]
    ).edges

    assert_equal([%w[a b], %w[b c], %w[c d]], edges.map { |edge| [edge.from, edge.to] })
    assert_raises(ArgumentError) { FlexiQ::WorkflowEdge.coerce(%w[a b c]) }
  end

  def test_every_dynamic_construct_has_its_wire_shape
    node = FlexiQ::WorkflowNode.new(
      name: "n", task_name: "t",
      gate: { timeout: 60, on_timeout: :reject, message: "ok?" }, cache: {}, fan_out: { items_from: "a" },
      fan_in: { from: "f" },
      sub_workflow: { name: "child", version: 2, graph: { nodes: [{ name: "c", task_name: "ct" }] },
                      deferred_node_names: ["c"] }
    )
    wire = node.to_wire

    assert_equal({ "timeout" => "60s", "onTimeout" => "ON_TIMEOUT_REJECT", "message" => "ok?" }, wire["gate"])
    assert_equal({}, wire["cache"])
    assert_equal({ "itemsFrom" => "a" }, wire["fanOut"])
    assert_equal({ "from" => "f" }, wire["fanIn"])
    child = wire["subWorkflow"]

    assert_equal ["child", 2, ["c"]], child.values_at("name", "version", "deferredNodeNames")
    assert_equal "c", child["graph"]["nodeConfigs"][0]["name"]
  end

  def test_condition_and_on_timeout_refuse_unknown_symbols
    assert_raises(ArgumentError) { FlexiQ::WorkflowNode.new(name: "n", task_name: "t", condition: :maybe).to_wire }
    assert_raises(ArgumentError) { FlexiQ::GateConfig.new(on_timeout: :later).to_wire }
    assert_equal "EDGE_CONDITION_ALWAYS", FlexiQ::EdgeCondition.dump(:always)
  end

  def test_a_node_needs_a_name_and_a_task
    assert_raises(ArgumentError) { FlexiQ::WorkflowNode.new(name: "", task_name: "t") }
    assert_raises(ArgumentError) { FlexiQ::WorkflowNode.new(name: "n", task_name: nil) }
  end

  def test_validate_accepts_a_well_formed_graph
    graph = two_nodes

    assert_same graph, graph.validate!
  end

  def test_validate_refuses_an_empty_graph_a_duplicate_and_a_dangling_edge
    node = { name: "a", task_name: "t" }

    assert_raises(ArgumentError) { FlexiQ::WorkflowGraph.new(nodes: []).validate! }
    error = assert_raises(ArgumentError) { FlexiQ::WorkflowGraph.new(nodes: [node, node]).validate! }
    assert_match(/"a" is declared twice/, error.message)
    error = assert_raises(ArgumentError) { FlexiQ::WorkflowGraph.new(nodes: [node], edges: [%w[a b]]).validate! }
    assert_match(/undeclared node "b"/, error.message)
  end
end
