# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/worker_harness"

# What an attached worker sends about each job it is dispatched: exactly one settling frame, the
# lease echoed, side-channel frames only where acknowledged.
class WorkerSettleTest < Minitest::Test
  include WorkerHarness

  def test_a_job_settles_once_with_its_result_and_lease
    attach = attached
    attach.dispatch(lease: "L1".b)
    success = attach.receive_arm(:success).success

    assert_equal [3, "L1"], [FlexiQ::Payload.decode_result(success.result), success.lease]
  end

  def test_side_channel_frames_precede_the_settle_and_echo_the_lease
    @handlers["add"] = lambda do |job|
      job.progress(40)
      job.log("info", "working")
      "done"
    end
    attach = attached
    attach.dispatch(lease: "L2".b)
    frames = Array.new(3) { attach.receive }

    assert_equal %i[progress task_log success], frames.map(&:frame)
    assert_equal(%w[L2 L2 L2], frames.map { _1.public_send(_1.frame).lease })
  end

  def test_side_channel_is_silent_when_not_acknowledged
    @handlers["add"] = lambda do |job|
      job.progress(40)
      1
    end
    attach = attached(capabilities: %w[lease])
    attach.dispatch

    assert_equal :success, attach.receive.frame
  end

  def test_an_unregistered_task_fails_without_a_retry
    attach = attached
    attach.dispatch(task_name: "nope")
    failure = attach.receive_arm(:failure).failure

    refute failure.should_retry
    assert_equal "TaskNotRegistered", FlexiQ::TaskError.parse(failure.error).errtype
  end

  def test_no_free_slot_fails_with_a_retry
    release = Queue.new
    @handlers["add"] = ->(_job) { release.pop }
    attach = attached(slots: 1)
    attach.dispatch(id: "first")
    attach.dispatch(id: "second")
    failure = attach.receive_arm(:failure).failure
    release << 1

    assert_equal ["second", true, "NoCapacity"],
                 [failure.job_id, failure.should_retry, FlexiQ::TaskError.parse(failure.error).errtype]
    assert_equal "first", attach.receive_arm(:success).success.job_id
  end

  def test_a_cancel_frame_settles_a_cooperating_handler_as_cancelled
    started = Queue.new
    @handlers["add"] = lambda do |job|
      started << true
      loop do
        job.check!
        sleep 0.01
      end
    end
    attach = attached
    attach.dispatch(lease: "L3".b)
    started.pop(timeout: 5)
    attach.send_frame(cancel: V1::CancelFrame.new(job_id: "job-1"))

    assert_equal "L3", attach.receive_arm(:cancelled).cancelled.lease
  end

  def test_a_timeout_is_enforced_cooperatively
    @handlers["add"] = lambda do |job|
      loop do
        job.check!
        sleep 0.01
      end
    end
    attach = attached
    attach.dispatch(timeout: Google::Protobuf::Duration.new(nanos: 100_000_000))
    failure = attach.receive_arm(:failure).failure

    assert failure.timed_out
    assert_equal "job timed out after 100ms", FlexiQ::TaskError.parse(failure.error).message
  end

  def test_unknown_and_unasked_frames_are_skipped
    attach = attached
    attach.send_frame
    attach.send_frame(job_steps: V1::JobStepsFrame.new(job_id: "x"))
    attach.dispatch

    assert_equal :success, attach.receive.frame
  end
end
