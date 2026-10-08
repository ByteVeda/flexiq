# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/fake_server"

class TransportStreamTest < Minitest::Test
  def setup
    @response = [200, ["data: a\n\n", "data: b\n\n"]]
    @server = FakeServer.new { |_request| @response }
  end

  def teardown = @server.stop

  def transport(**) = FlexiQ::Transport.new(@server.url, token: "t", insecure: true, **)

  def read_all(**)
    chunks = []
    transport(**).stream("/v1/jobs:watch", { "jobIds" => %w[a b], "queue" => nil }) { |chunk| chunks << chunk }
    chunks.join
  end

  def test_the_body_streams_to_the_block_until_the_server_closes
    assert_equal "data: a\n\ndata: b\n\n", read_all
    request = @server.last_request

    assert_equal "/v1/jobs:watch?jobIds=a&jobIds=b", request.path
    assert_equal ["text/event-stream", "Bearer t", "identity"],
                 request.headers.values_at("accept", "authorization", "accept-encoding")
  end

  def test_a_refusal_before_the_stream_opens_raises_like_any_call
    @response = [429, { "error" => { "code" => 429, "status" => "RESOURCE_EXHAUSTED", "message" => "cap",
                                     "details" => [{ "@type" => FlexiQ::RPCError::ERROR_INFO_TYPE,
                                                     "domain" => FlexiQ::Reason::DOMAIN,
                                                     "reason" => "WATCH_LIMIT", "metadata" => { "cap" => "4" } }] } }]
    error = assert_raises(FlexiQ::RPCError) { read_all }

    assert_equal [FlexiQ::Reason::WATCH_LIMIT, 4], [error.reason, error.metadata_integer("cap")]
  end

  def test_a_success_that_is_not_an_event_stream_is_refused
    @response = [200, { "job" => {} }]
    error = assert_raises(FlexiQ::TransportError) { read_all }

    assert_match(%r{not text/event-stream}, error.message)
  end

  def test_a_stream_silent_past_the_watch_read_timeout_is_dropped
    @response = [200, ->(socket) { sleep 2 if socket.write(": keepalive\n\n") }]
    started = FlexiQ::Deadline.now

    assert_raises(FlexiQ::TransportError) { read_all(watch_read_timeout: 0.2) }
    assert_operator FlexiQ::Deadline.now - started, :<, 1.5
  end

  def test_a_deadline_cuts_the_read_timeout_short
    @response = [200, ->(_socket) { sleep 2 }]
    started = FlexiQ::Deadline.now

    assert_raises(FlexiQ::TransportError) do
      transport.stream("/v1/jobs:watch", deadline: FlexiQ::Deadline.within(0.2)) { nil }
    end
    assert_operator FlexiQ::Deadline.now - started, :<, 1.5
  end
end
