# frozen_string_literal: true

require_relative "../test_helper"

class RPCErrorTest < Minitest::Test
  QUEUE_FULL = {
    "code" => 429, "status" => "RESOURCE_EXHAUSTED", "message" => "queue `payments` is full",
    "details" => [
      { "@type" => "type.googleapis.com/google.rpc.ErrorInfo", "reason" => "QUEUE_FULL",
        "domain" => "flexiq.byteveda.org",
        "metadata" => { "queue" => "payments", "pending" => "1001", "cap" => "1000" } },
      { "@type" => "type.googleapis.com/google.rpc.RetryInfo", "retryDelay" => "1s" }
    ]
  }.freeze

  def test_reads_reason_metadata_and_retry_info
    error = FlexiQ::RPCError.from_status(QUEUE_FULL)

    assert_equal [FlexiQ::Reason::QUEUE_FULL, "RESOURCE_EXHAUSTED", 429], [error.reason, error.code, error.http_status]
    assert_equal({ queue: "payments", pending: 1001, cap: 1000 }, error.queue_full)
    assert_equal 1, error.retry_after
    assert_predicate error, :retryable?
    assert_equal "QUEUE_FULL (RESOURCE_EXHAUSTED): queue `payments` is full", error.message
  end

  def test_error_info_from_another_domain_is_ignored
    status = QUEUE_FULL.merge("details" => [QUEUE_FULL["details"][0].merge("domain" => "example.com")])

    assert_nil FlexiQ::RPCError.from_status(status).reason
  end

  def test_unparseable_metadata_reads_as_absent
    info = QUEUE_FULL["details"][0].merge("metadata" => { "queue" => "q", "pending" => "lots", "cap" => "1" })
    error = FlexiQ::RPCError.from_status(QUEUE_FULL.merge("details" => [info]))

    assert_nil error.metadata_integer("pending")
    assert_nil error.queue_full
  end

  def test_batch_index_and_scope
    info = { "@type" => FlexiQ::RPCError::ERROR_INFO_TYPE, "domain" => FlexiQ::Reason::DOMAIN,
             "reason" => "SCOPE_DENIED", "metadata" => { "scope" => "produce", "index" => "2" } }
    error = FlexiQ::RPCError.from_status({ "status" => "PERMISSION_DENIED", "message" => "m", "details" => [info] })

    assert_equal [2, "produce"], [error.batch_index, error.scope]
    refute_predicate error, :retryable?
  end

  def test_reasonless_unavailable_is_retryable_but_a_reasoned_internal_is_not
    assert_predicate FlexiQ::RPCError.from_http(503, "<html>"), :retryable?
    assert_equal "DEADLINE_EXCEEDED", FlexiQ::RPCError.from_http(504, "").code
    refute_predicate FlexiQ::RPCError.new("m", code: "INTERNAL", reason: "INTERNAL"), :retryable?
  end
end
