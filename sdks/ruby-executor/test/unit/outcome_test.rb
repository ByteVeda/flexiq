# frozen_string_literal: true

require_relative "../test_helper"

class OutcomeTest < Minitest::Test
  def setup
    @job = FlexiQ::Executor::Job.new(job_frame(lease: "lease".b, timeout: Google::Protobuf::Duration.new(seconds: 3)))
  end

  def test_a_value_is_a_success_with_its_cbor_result
    success = frame(value: { "sum" => 3 }).success

    assert_equal({ "sum" => 3 }, FlexiQ::Payload.decode_result(success.result))
    assert_equal %w[job-1 add lease], [success.job_id, success.task_name, success.lease]
    assert_equal [1, 250_000_000], [success.wall_time.seconds, success.wall_time.nanos]
  end

  def test_nil_is_a_success_with_no_result
    refute_predicate frame(value: nil).success, :has_result?
  end

  def test_a_value_that_does_not_encode_fails_without_a_retry
    failure = frame(value: Object.new).failure

    refute failure.should_retry
    assert_equal "ResultEncodeError", FlexiQ::TaskError.parse(failure.error).errtype
  end

  def test_an_exception_is_a_retryable_failure_in_the_canonical_shape
    failure = frame(error: raised(ArgumentError, "bad value 42")).failure
    error = FlexiQ::TaskError.parse(failure.error)

    assert failure.should_retry
    assert_equal ["ArgumentError", "bad value 42"], [error.errtype, error.message]
    refute_empty error.traceback
    assert_equal [1, 3, "lease"], [failure.retry_count, failure.max_retries, failure.lease]
  end

  def test_fatal_does_not_retry_and_names_its_cause
    error = begin
      begin
        raise KeyError, "no key"
      rescue KeyError
        raise FlexiQ::Executor::Fatal, "giving up"
      end
    rescue FlexiQ::Executor::Fatal => e
      e
    end
    failure = frame(error: error).failure

    refute failure.should_retry
    assert_equal %w[KeyError], [FlexiQ::TaskError.parse(failure.error).errtype]
  end

  def test_a_timeout_reads_like_the_one_core_writes
    failure = frame(error: raised(RuntimeError, "slow"), timed_out: true).failure

    assert failure.timed_out
    assert failure.should_retry
    error = FlexiQ::TaskError.parse(failure.error)

    assert_equal ["TimeoutError", "job timed out after 3000ms"], [error.errtype, error.message]
  end

  def test_a_value_returned_past_the_timeout_is_still_a_success
    assert_equal :success, frame(value: 1, timed_out: true).frame
  end

  def test_cancelled_settles_only_when_the_handler_raised_cancelled
    assert_equal "lease", frame(error: raised(FlexiQ::Executor::Cancelled, "x"), cancelled: true).cancelled.lease
    assert_equal :failure, frame(error: raised(RuntimeError, "x"), cancelled: true).frame
    assert_equal :failure, frame(error: raised(FlexiQ::Executor::Cancelled, "x")).frame
  end

  def test_a_failure_without_a_lease_carries_none
    job = FlexiQ::Executor::Job.new(job_frame)
    failure = FlexiQ::Executor::Outcome.failure(job, "{}", should_retry: true).failure

    refute_predicate failure, :has_lease?
    assert_nil failure.wall_time
  end

  private

  def frame(value: nil, error: nil, timed_out: false, cancelled: false)
    FlexiQ::Executor::Outcome.new(value: value, error: error, wall: 1.25, timed_out: timed_out,
                                  cancelled: cancelled).frame(@job)
  end

  def raised(klass, message)
    raise klass, message
  rescue klass => e
    e
  end
end
