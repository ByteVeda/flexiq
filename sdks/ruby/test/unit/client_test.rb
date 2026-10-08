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
