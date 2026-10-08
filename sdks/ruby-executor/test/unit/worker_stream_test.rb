# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/worker_harness"

# The attach stream's lifecycle: handshake order, heartbeats, and each way a stream ends.
class WorkerStreamTest < Minitest::Test
  include WorkerHarness

  def test_hello_goes_first_and_nothing_else_until_the_ack
    start(slots: 3, id: "ruby-1")
    attach = @scheduler.next_attach
    hello = attach.receive.hello

    assert_equal ["ruby-1", "ruby", %w[add], 3, 1, %w[side_channel lease]],
                 [hello.executor_id, hello.sdk, hello.tasks.to_a, hello.slots, hello.protocol_version,
                  hello.capabilities.to_a]
    assert_equal "Bearer token", attach.metadata["authorization"]
    sleep 0.3

    assert_empty @scheduler.heartbeats, "a heartbeat must not overtake hello_ack"
  end

  def test_heartbeat_carries_the_session_token_verbatim
    attached
    request, metadata = @scheduler.heartbeats.pop(timeout: 5)

    assert_equal [FakeScheduler::SESSION, 2], [request.session, request.free_slots]
    assert_equal "Bearer token", metadata["authorization"]
  end

  def test_a_clean_stream_end_is_a_rotation_and_reconnects
    attached.finish
    second = @scheduler.next_attach

    assert_equal :hello, second.receive.frame
  end

  def test_a_shutdown_frame_drains_and_returns
    release = Queue.new
    @handlers["add"] = ->(_job) { release.pop }
    attach = attached
    attach.dispatch
    sleep 0.1
    attach.send_frame(shutdown: V1::ShutdownFrame.new)
    sleep 0.1
    release << 1

    assert_equal :success, attach.receive_arm(:success).frame
    attach.await_half_close

    assert @runner.join(10)
    assert_nil @runner.value
  end

  def test_stop_reports_zero_capacity_drains_running_jobs_and_returns
    release = Queue.new
    @handlers["add"] = ->(_job) { release.pop }
    attach = attached
    attach.dispatch
    sleep 0.1
    @worker.stop

    assert_equal 0, await_heartbeat { _1.free_slots.zero? }.free_slots
    release << 1

    assert_equal :success, attach.receive_arm(:success).frame
    attach.await_half_close

    assert @runner.join(10)
  end

  def test_a_permanent_refusal_is_raised
    start
    attach = @scheduler.next_attach
    attach.receive
    attach.fail_with(GRPC::Core::StatusCodes::UNAUTHENTICATED, "bad token")

    error = assert_raises(FlexiQ::Executor::AttachError) { @runner.value }
    assert_predicate error, :permanent?
    assert_match(/bad token/, error.message)
  end

  def test_a_transient_failure_reconnects
    start
    first = @scheduler.next_attach
    first.receive
    first.fail_with(GRPC::Core::StatusCodes::UNAVAILABLE, "restarting")

    assert_equal :hello, @scheduler.next_attach.receive.frame
  end

  def test_an_id_still_attached_reconnects
    start
    first = @scheduler.next_attach
    first.receive
    first.fail_with(GRPC::Core::StatusCodes::ALREADY_EXISTS, "wait for the previous stream to end")

    assert_equal :hello, @scheduler.next_attach.receive.frame
  end

  def test_a_protocol_version_mismatch_is_permanent
    start
    attach = @scheduler.next_attach
    attach.receive
    attach.ack(protocol_version: 2)

    error = assert_raises(FlexiQ::Executor::AttachError) { @runner.value }
    assert_match(/we speak 1, the scheduler speaks 2/, error.message)
  end

  def test_a_first_frame_other_than_the_ack_is_permanent
    start
    attach = @scheduler.next_attach
    attach.receive
    attach.dispatch

    error = assert_raises(FlexiQ::Executor::AttachError) { @runner.value }
    assert_match(/first frame was not a hello acknowledgement/, error.message)
  end

  def test_a_missing_ack_times_out_and_reconnects
    start(handshake_timeout: 0.2)
    @scheduler.next_attach.receive

    assert_equal :hello, @scheduler.next_attach.receive.frame
  end
end
