# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/fake_server"

class ClientTest < Minitest::Test
  TOKEN = "fqt_0123456789abcdef.secret"
  JOB = { "id" => "j1", "queue" => "default", "taskName" => "send", "status" => "JOB_STATUS_PENDING" }.freeze

  def setup
    @response = [200, { "job" => JOB, "deduplicated" => false }]
    @server = FakeServer.new { |_request| @response }
    @client = FlexiQ::Client.new(@server.url, token: TOKEN, insecure: true)
  end

  def teardown
    @client.close
    @server.stop
  end

  def test_enqueue_posts_the_raw_envelope_with_bearer_auth
    result = @client.enqueue("send", args: [1, "a"], queue: "emails", unique_key: "k")
    request = @server.last_request

    assert_equal ["POST", "/v1/jobs"], [request.verb, request.path]
    assert_equal "Bearer #{TOKEN}", request.headers["authorization"]
    assert_match %r{\Aflexiq-ruby/}, request.headers["user-agent"]
    assert_equal({ "taskName" => "send", "raw" => "AoKCAWFhoA==",
                   "options" => { "queue" => "emails", "uniqueKey" => "k" } }, request.json)
    assert_equal ["j1", :pending], [result.job.id, result.job.status]
    refute_predicate result, :deduplicated?
  end

  def test_enqueue_batch_reports_each_item
    @response = [200, { "results" => [
      { "enqueued" => { "job" => JOB, "deduplicated" => true } },
      { "error" => { "code" => 429, "status" => "RESOURCE_EXHAUSTED", "message" => "full", "details" => [] } },
      {}
    ] }]
    results = @client.enqueue_batch([FlexiQ::EnqueueRequest.new(task_name: "a"), { task_name: "b" },
                                     { task_name: "c" }])

    assert_equal "/v1/jobs:batchEnqueue", @server.last_request.path
    assert_equal [true, false, false], results.map(&:enqueued?)
    assert_predicate results[0].enqueued, :deduplicated?
    assert_equal "RESOURCE_EXHAUSTED", results[1].error.code
    refute_predicate results[2], :failed?
  end

  def test_a_batch_answer_that_does_not_pair_with_the_request_is_refused
    [{ "results" => [] }, { "results" => [{}, {}] }, {}].each do |body|
      @response = [200, body]
      assert_raises(FlexiQ::TransportError, body.inspect) { @client.enqueue_batch([{ task_name: "a" }]) }
    end
  end

  def test_get_job_escapes_the_id_and_sends_blob_switches
    @response = [200, { "job" => JOB }]
    @client.get_job("a/b", include_result: true)
    request = @server.last_request

    assert_equal ["GET", "/v1/jobs/a%2Fb?includeResult=true"], [request.verb, request.path]
  end

  def test_cancel_job_posts_the_custom_verb
    @response = [200, { "job" => JOB.merge("cancelRequested" => true) }]

    assert_predicate @client.cancel_job("j1"), :cancel_requested?
    request = @server.last_request

    assert_equal ["POST", "/v1/jobs/j1:cancel"], [request.verb, request.path]
  end

  def test_queue_stats_per_queue_and_namespace_wide
    @response = [200, { "pending" => "3", "running" => "1", "completed" => "9007199254740993",
                        "failed" => "0", "dead" => "0", "cancelled" => "0" }]
    stats = @client.queue_stats("emails")

    assert_equal "/v1/queues/emails/stats", @server.last_request.path
    assert_equal [3, 9_007_199_254_740_993], [stats.pending, stats.completed]

    @client.queue_stats

    assert_equal "/v1/stats", @server.last_request.path
  end

  def test_list_jobs_sends_filters_and_drops_the_unset_ones
    @response = [200, { "jobs" => [JOB], "nextPageToken" => "" }]
    page = @client.list_jobs(status: :failed, task_name: "send", page_size: 2)

    assert_equal "/v1/jobs?status=JOB_STATUS_FAILED&taskName=send&pageSize=2", @server.last_request.path
    assert_equal ["j1"], page.jobs.map(&:id)
    assert_predicate page, :last_page?
    assert_raises(ArgumentError) { @client.list_jobs(status: :finished) }
    assert_raises(ArgumentError) { @client.list_jobs(status: false) }
  end

  def test_each_job_follows_the_page_token_to_the_end
    pages = {
      nil => { "jobs" => [JOB.merge("id" => "j3"), JOB.merge("id" => "j2")], "nextPageToken" => "c/2+=" },
      "c/2+=" => { "jobs" => [JOB.merge("id" => "j1")], "nextPageToken" => "" }
    }
    server = FakeServer.new do |request|
      query = URI.decode_www_form(URI(request.path).query.to_s).to_h
      [200, pages.fetch(query["pageToken"])]
    end
    client = FlexiQ::Client.new(server.url, token: TOKEN, insecure: true)
    jobs = client.each_job(queue: "emails", page_size: 2)

    assert_kind_of Enumerator, jobs
    assert_equal %w[j3 j2 j1], jobs.map(&:id)
    assert_equal ["/v1/jobs?queue=emails&pageSize=2", "/v1/jobs?queue=emails&pageSize=2&pageToken=c%2F2%2B%3D"],
                 [server.last_request.path, server.last_request.path]
  ensure
    client&.close
    server&.stop
  end

  def test_a_token_that_does_not_decode_surfaces_as_the_servers_refusal
    @response = [400, { "error" => { "code" => 400, "status" => "INVALID_ARGUMENT", "message" => "bad page_token",
                                     "details" => [{ "@type" => FlexiQ::RPCError::ERROR_INFO_TYPE,
                                                     "domain" => FlexiQ::Reason::DOMAIN,
                                                     "reason" => "INVALID_REQUEST" }] } }]
    error = assert_raises(FlexiQ::RPCError) { @client.list_jobs(page_token: "forged") }

    assert_equal [FlexiQ::Reason::INVALID_REQUEST, "INVALID_ARGUMENT"], [error.reason, error.code]
    assert_equal "/v1/jobs?pageToken=forged", @server.last_request.path
  end

  def test_a_listing_without_a_job_list_is_a_transport_error
    @response = [200, { "jobs" => "nope" }]
    assert_raises(FlexiQ::TransportError) { @client.list_jobs }
  end

  def test_submit_workflow_posts_the_graph_and_returns_the_run_id
    @response = [200, { "runId" => "r1" }]
    graph = FlexiQ::WorkflowGraph.new(nodes: [{ name: "a", task_name: "t" }])
    run_id = @client.submit_workflow("flow", graph, params_json: '{"k":1}')
    request = @server.last_request

    assert_equal "r1", run_id
    assert_equal ["POST", "/v1/workflows"], [request.verb, request.path]
    assert_equal({ "name" => "flow", "graph" => graph.to_wire, "paramsJson" => '{"k":1}' }, request.json)
  end

  def test_submit_workflow_refuses_a_bad_call_before_sending_it
    graph = FlexiQ::WorkflowGraph.new(nodes: [{ name: "a", task_name: "t" }], edges: [%w[a b]])

    assert_raises(ArgumentError) { @client.submit_workflow("flow", graph) }
    assert_raises(ArgumentError) { @client.submit_workflow("", FlexiQ::WorkflowGraph.new(nodes: [])) }
    assert_raises(ArgumentError) { @client.submit_workflow("flow", { nodes: [] }) }
    assert_raises(ArgumentError) do
      @client.submit_workflow("flow", FlexiQ::WorkflowGraph.new(nodes: [{ name: "a", task_name: "t" }]),
                              params_json: { "k" => 1 })
    end
    assert_empty @server.requests
  end

  def test_a_submission_answer_without_a_run_id_is_a_transport_error
    @response = [200, {}]
    graph = FlexiQ::WorkflowGraph.new(nodes: [{ name: "a", task_name: "t" }])

    assert_raises(FlexiQ::TransportError) { @client.submit_workflow("flow", graph) }
  end

  def test_get_workflow_run_escapes_the_id
    @response = [200, { "run" => { "id" => "r/1", "state" => "WORKFLOW_STATE_PENDING" }, "nodes" => [] }]
    view = @client.get_workflow_run("r/1")
    request = @server.last_request

    assert_equal ["GET", "/v1/workflows/r%2F1"], [request.verb, request.path]
    assert_equal ["r/1", :pending], [view.run.id, view.run.state]
  end

  def test_a_refusal_raises_rpc_error_with_its_reason
    @response = [404, { "error" => { "code" => 404, "status" => "NOT_FOUND", "message" => "no such job",
                                     "details" => [{ "@type" => FlexiQ::RPCError::ERROR_INFO_TYPE,
                                                     "domain" => FlexiQ::Reason::DOMAIN,
                                                     "reason" => "JOB_NOT_FOUND" }] } }]
    error = assert_raises(FlexiQ::RPCError) { @client.get_job("nope") }

    assert_equal [FlexiQ::Reason::JOB_NOT_FOUND, 404], [error.reason, error.http_status]
  end

  def test_a_non_flexiq_failure_still_raises_rpc_error
    @response = [502, "<html>bad gateway</html>"]
    error = assert_raises(FlexiQ::RPCError) { @client.queue_stats }

    assert_nil error.reason
    assert_equal "INTERNAL", error.code
  end

  def test_a_non_json_success_is_a_transport_error
    @response = [200, "ok"]
    assert_raises(FlexiQ::TransportError) { @client.queue_stats }
  end

  def test_a_dead_server_is_a_transport_error
    @server.stop
    assert_raises(FlexiQ::TransportError) { @client.queue_stats }
  end
end
