# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/fake_server"

class WaitTest < Minitest::Test
  END_EVENT = "event: end\ndata: {}\n\n"
  JOB = { "id" => "a", "queue" => "q", "taskName" => "t", "status" => "JOB_STATUS_COMPLETE",
          "result" => "AQID" }.freeze

  def setup
    @watch = [200, [snapshot("COMPLETE", terminal: true), END_EVENT]]
    @reads = [[200, { "job" => JOB }]]
    @server = FakeServer.new do |request|
      next @watch if request.path.start_with?("/v1/jobs:watch")

      sleep @read_delay if @read_delay
      @reads.length > 1 ? @reads.shift : @reads.first
    end
    @client = FlexiQ::Client.new(@server.url, token: "t", insecure: true)
  end

  def teardown
    @client.close
    @server.stop
  end

  def snapshot(status, terminal: false)
    json = { "transition" => { "jobId" => "a", "kind" => "JOB_TRANSITION_KIND_SNAPSHOT",
                               "status" => "JOB_STATUS_#{status}", "terminal" => terminal }, "cursor" => "" }
    "data: #{JSON.generate(json)}\n\n"
  end

  def not_found
    { "error" => { "code" => 404, "status" => "NOT_FOUND", "message" => "no",
                   "details" => [{ "@type" => FlexiQ::RPCError::ERROR_INFO_TYPE, "domain" => FlexiQ::Reason::DOMAIN,
                                   "reason" => "JOB_NOT_FOUND" }] } }
  end

  def paths = Array.new(@server.requests.size) { @server.requests.pop.path }

  def elapsed
    started = FlexiQ::Deadline.now
    yield
    FlexiQ::Deadline.now - started
  end

  def test_a_finished_job_reads_back_with_its_result
    job = @client.wait("a", timeout: 5)

    assert_equal [:complete, "\x01\x02\x03".b], [job.status, job.result]
    assert_equal ["/v1/jobs:watch?jobIds=a", "/v1/jobs/a?includeResult=true"], paths
  end

  def test_a_job_this_token_cannot_see_raises_job_not_found
    @watch = [200, ["data: {\"notFoundJobId\":\"a\",\"cursor\":\"\"}\n\n", END_EVENT]]
    @reads = [[404, not_found]]
    error = assert_raises(FlexiQ::RPCError) { @client.wait("a", timeout: nil) }

    assert_equal FlexiQ::Reason::JOB_NOT_FOUND, error.reason
  end

  def test_a_read_that_fails_transiently_is_retried
    @reads = [[503, "upstream down"], [200, { "job" => JOB }]]

    assert_equal "a", @client.wait("a", timeout: 5).id
  end

  def test_keepalives_do_not_stretch_the_timeout
    @watch = [200, lambda do |socket|
      socket.write(snapshot("RUNNING"))
      20.times do
        socket.write(": keepalive\n\n")
        sleep 0.1
      end
    end]

    took = elapsed { assert_raises(FlexiQ::WaitTimeoutError) { @client.wait("a", timeout: 0.4) } }

    assert_operator took, :<, 1.5
  end

  def test_a_silent_stream_times_out_as_a_wait_not_a_drop
    @watch = [200, lambda do |socket|
      socket.write(snapshot("RUNNING"))
      sleep 2
    end]

    took = elapsed { assert_raises(FlexiQ::WaitTimeoutError) { @client.wait("a", timeout: 0.3) } }

    assert_operator took, :<, 1.5
  end

  def test_a_stalled_final_read_is_bounded_by_the_timeout
    @read_delay = 2

    took = elapsed { assert_raises(FlexiQ::WaitTimeoutError) { @client.wait("a", timeout: 0.5) } }

    assert_operator took, :<, 1.5
  end

  def test_a_timeout_must_be_positive_seconds_or_nil
    [0, -1, "5"].each do |timeout|
      assert_raises(ArgumentError, timeout.inspect) { @client.wait("a", timeout: timeout) }
    end
  end
end
