# frozen_string_literal: true

require_relative "../test_helper"

class JobTest < Minitest::Test
  def test_fields_come_from_the_frame
    job = FlexiQ::Executor::Job.new(job_frame(namespace: "tenant", metadata: "{}",
                                              timeout: Google::Protobuf::Duration.new(seconds: 2, nanos: 500_000_000)))

    assert_equal ["job-1", "add", "work", "tenant", 1, 3, "{}"],
                 [job.id, job.task_name, job.queue, job.namespace, job.retry_count, job.max_retries, job.metadata]
    assert_in_delta 2.5, job.timeout
    assert_equal [[1, 2], { "k" => "v" }], [job.args, job.kwargs]
  end

  def test_absent_optional_fields_are_nil
    job = FlexiQ::Executor::Job.new(job_frame)

    assert_nil job.namespace
    assert_nil job.metadata
    assert_nil job.timeout
    assert_nil job.deadline
    assert_nil job.lease
    refute_predicate job, :timed_out?
  end

  def test_an_undecodable_payload_raises_on_first_use
    job = FlexiQ::Executor::Job.new(job_frame(payload: "\x01\x90".b))

    assert_raises(FlexiQ::CodecError) { job.args }
  end

  def test_check_raises_for_cancel_then_timeout
    job = FlexiQ::Executor::Job.new(job_frame(timeout: Google::Protobuf::Duration.new(seconds: 1)), now: 0)

    assert_raises(FlexiQ::Executor::TimeoutError) { job.check! }
    job.request_cancel!

    assert_raises(FlexiQ::Executor::Cancelled) { job.check! }
  end

  def test_progress_is_clamped_and_carries_the_lease
    side = FlexiQ::Executor::Outbox.new
    job = FlexiQ::Executor::Job.new(job_frame(lease: "L".b), side: side)
    job.progress(150)

    frame = drain(side).first.progress

    assert_equal [100, "L"], [frame.progress, frame.lease]
  end

  def test_log_encodes_extra_as_json
    side = FlexiQ::Executor::Outbox.new
    job = FlexiQ::Executor::Job.new(job_frame(lease: "L".b), side: side)
    job.log(:warn, "careful", extra: { "n" => 1 })
    job.publish([1, 2])

    first, second = drain(side).map(&:task_log)

    assert_equal ["warn", "careful", '{"n":1}', "L"], [first.level, first.message, first.extra, first.lease]
    assert_equal ["result", "", "[1,2]"], [second.level, second.message, second.extra]
  end

  def test_side_channel_calls_are_no_ops_without_the_capability
    job = FlexiQ::Executor::Job.new(job_frame)

    assert_nil job.progress(5)
    assert_nil job.log("info", "x")
  end

  def test_an_unknown_log_level_is_refused
    job = FlexiQ::Executor::Job.new(job_frame)

    assert_raises(ArgumentError) { job.log("debug", "x") }
  end

  private

  def drain(outbox)
    outbox.close
    outbox.frames(-> { false }).to_a
  end
end
