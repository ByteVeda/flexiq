# frozen_string_literal: true

require_relative "../test_helper"

class WorkflowRunTest < Minitest::Test
  RESPONSE = {
    "run" => {
      "id" => "r1", "definitionId" => "d1", "state" => "WORKFLOW_STATE_RUNNING",
      "createdAt" => "2026-10-08T10:00:00Z", "startedAt" => "2026-10-08T10:00:01.500Z"
    },
    "nodes" => [
      { "name" => "a", "status" => "WORKFLOW_NODE_STATUS_COMPLETED", "jobId" => "j1",
        "completedAt" => "2026-10-08T10:00:02Z" },
      { "name" => "b", "status" => "WORKFLOW_NODE_STATUS_PENDING" }
    ]
  }.freeze

  def test_a_run_and_its_nodes_read_back
    view = FlexiQ::WorkflowRunView.from_json(RESPONSE)

    assert_equal %w[r1 d1], [view.run.id, view.run.definition_id]
    assert_equal :running, view.run.state
    assert_equal Time.utc(2026, 10, 8, 10, 0, 1.5r), view.run.started_at
    assert_nil view.run.completed_at
    refute_predicate view.run, :terminal?
    assert_equal [:completed, "j1", true], [view.node("a").status, view.node("a").job_id, view.node("a").terminal?]
    assert_equal [:pending, nil], [view.node("b").status, view.node("b").job_id]
    assert_nil view.node("missing")
  end

  def test_an_unknown_state_stays_raw_and_is_never_terminal
    run = FlexiQ::WorkflowRun.from_json({ "id" => "r", "state" => "WORKFLOW_STATE_ORBITING" })
    node = FlexiQ::WorkflowNodeState.from_json({ "name" => "n", "status" => 42 })

    assert_equal "WORKFLOW_STATE_ORBITING", run.state
    refute_predicate run, :terminal?
    refute FlexiQ::WorkflowState.known?(run.state)
    assert_equal 42, node.status
    refute_predicate node, :terminal?
  end

  def test_terminal_states_match_the_core
    assert_predicate FlexiQ::WorkflowRun.from_json({ "state" => "WORKFLOW_STATE_COMPLETED_WITH_FAILURES" }), :terminal?
    refute_predicate FlexiQ::WorkflowRun.from_json({ "state" => "WORKFLOW_STATE_COMPENSATING" }), :terminal?
    assert_predicate FlexiQ::WorkflowNodeState.from_json({ "status" => "WORKFLOW_NODE_STATUS_SKIPPED" }), :terminal?
    refute_predicate FlexiQ::WorkflowNodeState.from_json({ "status" => "WORKFLOW_NODE_STATUS_WAITING_APPROVAL" }),
                     :terminal?
  end

  def test_a_run_with_no_nodes_key_reads_as_empty
    view = FlexiQ::WorkflowRunView.from_json({ "run" => { "id" => "r" } })

    assert_empty view.nodes
  end

  def test_an_answer_without_a_run_is_a_transport_error
    assert_raises(FlexiQ::TransportError) { FlexiQ::WorkflowRunView.from_json({}) }
    assert_raises(FlexiQ::TransportError) { FlexiQ::WorkflowRunView.from_json({ "run" => {}, "nodes" => {} }) }
  end
end
