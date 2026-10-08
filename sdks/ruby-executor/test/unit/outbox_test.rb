# frozen_string_literal: true

require_relative "../test_helper"

class OutboxTest < Minitest::Test
  def setup
    @outbox = FlexiQ::Executor::Outbox.new
  end

  def test_settling_frames_go_before_side_channel_frames
    @outbox.push_side("a", log("a", "1"), progress: false)
    @outbox.push(settle("b"))

    assert_equal %i[cancelled task_log], drain.map(&:frame)
  end

  def test_progress_coalesces_per_job
    @outbox.push_side("a", progress("a", 10), progress: true)
    @outbox.push_side("b", progress("b", 20), progress: true)
    @outbox.push_side("a", progress("a", 30), progress: true)

    assert_equal([%w[a 30], %w[b 20]], drain.map { [_1.progress.job_id, _1.progress.progress.to_s] })
  end

  def test_the_oldest_side_frame_is_dropped_past_the_limit
    (FlexiQ::Executor::Outbox::SIDE_LIMIT + 2).times { |i| @outbox.push_side("a", log("a", i.to_s), progress: false) }

    messages = drain.map { _1.task_log.message }

    assert_equal 2, @outbox.dropped
    assert_equal %w[2 3], messages.first(2)
    assert_equal FlexiQ::Executor::Outbox::SIDE_LIMIT, messages.length
  end

  def test_promote_moves_a_jobs_side_frames_ahead_of_its_settle_in_order
    @outbox.push_side("a", log("a", "1"), progress: false)
    @outbox.push_side("b", log("b", "x"), progress: false)
    @outbox.push_side("a", log("a", "2"), progress: false)
    @outbox.promote("a")
    @outbox.push(settle("a"))

    assert_equal([%w[task_log 1], %w[task_log 2], ["cancelled", nil], %w[task_log x]],
                 drain.map { [_1.frame.to_s, _1.task_log&.message] })
  end

  def test_an_ended_stream_stops_the_writer_even_with_frames_queued
    @outbox.push(settle("a"))

    assert_empty @outbox.frames(-> { true }).to_a
  end

  def test_the_writer_waits_for_frames_until_closed
    received = Queue.new
    writer = Thread.new { @outbox.frames(-> { false }).each { received << _1 } }
    @outbox.push(settle("a"))

    assert_equal "a", received.pop(timeout: 2).cancelled.job_id
    @outbox.close

    assert writer.join(2)
    assert @outbox.await_drained(0)
  end

  def test_await_drained_times_out_while_the_writer_is_open
    refute @outbox.await_drained(0.05)
  end

  private

  def drain
    @outbox.close
    @outbox.frames(-> { false }).to_a
  end

  def settle(id) = V1::AttachRequest.new(cancelled: V1::CancelledFrame.new(job_id: id))

  def log(id, message) = V1::AttachRequest.new(task_log: V1::TaskLogFrame.new(job_id: id, message: message))

  def progress(id, value) = V1::AttachRequest.new(progress: V1::ProgressFrame.new(job_id: id, progress: value))
end
