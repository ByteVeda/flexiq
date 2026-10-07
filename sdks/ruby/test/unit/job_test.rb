# frozen_string_literal: true

require_relative "../test_helper"

class JobTest < Minitest::Test
  FULL = {
    "id" => "j1", "queue" => "emails", "taskName" => "send", "status" => "JOB_STATUS_COMPLETE",
    "priority" => 5, "createdAt" => "2026-01-02T03:04:05Z", "retryCount" => 1, "maxRetries" => 3,
    "timeout" => "30s", "cancelRequested" => false, "hasDeps" => true, "namespace" => "prod",
    "payload" => "AoKCAWFhoA==", "result" => "AvU=", "uniqueKey" => "k", "resultTtl" => "1.500s",
    "enqueuedBy" => "fqt_0123456789abcdef", "someFutureField" => "ignored"
  }.freeze

  def test_a_populated_job_maps_every_field
    job = FlexiQ::Job.from_json(FULL)

    assert_equal %w[j1 emails send prod k], [job.id, job.queue, job.task_name, job.namespace, job.unique_key]
    assert_equal :complete, job.status
    assert_equal Time.utc(2026, 1, 2, 3, 4, 5), job.created_at
    assert_equal [30, Rational(3, 2)], [job.timeout, job.result_ttl]
    assert_equal [[1, "a"], {}], job.decode_payload
    assert job.decode_result
    assert_predicate job, :terminal?
    assert job.has_deps
  end

  def test_an_unknown_status_is_kept_and_never_terminal
    %w[JOB_STATUS_ARCHIVED JOB_STATUS_UNSPECIFIED].push(9).each do |wire|
      job = FlexiQ::Job.from_json("id" => "j", "status" => wire)

      assert_equal wire, job.status
      refute_predicate job, :terminal?
      refute_predicate job, :known_status?
    end
  end

  def test_failed_is_not_terminal
    refute_predicate FlexiQ::Job.from_json("id" => "j", "status" => "JOB_STATUS_FAILED"), :terminal?
  end

  def test_missing_blobs_say_how_to_request_them
    job = FlexiQ::Job.from_json("id" => "j")
    error = assert_raises(FlexiQ::CodecError) { job.decode_payload }
    assert_match(/include_payload: true/, error.message)
    assert_raises(FlexiQ::CodecError) { job.decode_result }
    assert_nil job.task_error
  end
end
