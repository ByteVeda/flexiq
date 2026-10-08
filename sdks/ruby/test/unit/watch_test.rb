# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/fake_server"

# The watch over a fake JSON door that streams scripted event bodies, one per request.
class WatchTest < Minitest::Test
  END_EVENT = "event: end\ndata: {}\n\n"

  def setup
    @responses = []
    @server = FakeServer.new { |_request| @responses.length > 1 ? @responses.shift : @responses.first }
    @client = FlexiQ::Client.new(@server.url, token: "t", insecure: true)
  end

  def teardown
    @client.close
    @server.stop
  end

  def event(json, id: nil) = "data: #{JSON.generate(json)}\n#{"id: #{id}\n" if id}\n"

  def transition(job_id, kind, status, terminal: false, cursor: "")
    event({ "transition" => { "jobId" => job_id, "queue" => "q", "taskName" => "t",
                              "kind" => "JOB_TRANSITION_KIND_#{kind}", "status" => "JOB_STATUS_#{status}",
                              "attempt" => 1, "time" => "2026-10-08T10:00:00Z", "terminal" => terminal },
            "cursor" => cursor }, id: cursor.empty? ? nil : cursor)
  end

  def checkpoint(cursor) = event({ "cursor" => cursor }, id: cursor)

  def status(reason, code, http, metadata = {})
    { "code" => http, "status" => code, "message" => "no",
      "details" => [{ "@type" => FlexiQ::RPCError::ERROR_INFO_TYPE, "domain" => FlexiQ::Reason::DOMAIN,
                      "reason" => reason, "metadata" => metadata }] }
  end

  def error_event(reason, code, http)
    "event: error\ndata: #{JSON.generate("error" => status(reason, code, http))}\n\n"
  end

  def requests = Array.new(@server.requests.size) { @server.requests.pop }

  def query(request) = URI.decode_www_form(request.path.split("?", 2)[1].to_s)

  def test_an_id_watch_yields_every_change_until_each_job_is_terminal
    @responses << [200, [transition("a", "SNAPSHOT", "PENDING"), event({ "notFoundJobId" => "b", "cursor" => "" }),
                         ": keepalive\n\n", transition("a", "STARTED", "RUNNING"),
                         transition("a", "FAILED", "FAILED"), transition("a", "RETRYING", "PENDING"),
                         transition("a", "COMPLETED", "COMPLETE", terminal: true), END_EVENT]]
    items = []

    assert_nil(@client.watch_jobs(%w[a b a]) { |item| items << item })
    assert_equal [%w[jobIds a], %w[jobIds b]], query(requests.first)
    assert_equal FlexiQ::JobNotFound.new(job_id: "b"), items[1]
    transitions = items.grep(FlexiQ::JobTransition)

    assert_equal %i[snapshot started failed retrying completed], transitions.map(&:kind)
    assert_equal [false, false, false, false, true], transitions.map(&:terminal?)
    assert_equal [:failed, 1, Time.utc(2026, 10, 8, 10), nil],
                 transitions[2].to_h.values_at(:status, :attempt, :time, :cursor)
  end

  def test_a_failed_status_is_not_finished_and_a_reopen_watches_only_unfinished_jobs
    @responses << [200, [transition("a", "SNAPSHOT", "COMPLETE", terminal: true),
                         transition("b", "FAILED", "FAILED"), END_EVENT]]
    @responses << [200, [transition("b", "DEAD", "DEAD", terminal: true), END_EVENT]]
    items = @client.watch_jobs(%w[a b]).to_a

    assert_equal %i[snapshot failed dead], items.map(&:kind)
    assert_equal([[%w[jobIds a], %w[jobIds b]], [%w[jobIds b]]], requests.map { |request| query(request) })
  end

  def test_overflow_and_shutdown_reopen_the_watch
    @responses << [200,
                   [transition("a", "SNAPSHOT", "PENDING"), error_event("WATCH_OVERFLOW", "RESOURCE_EXHAUSTED", 429)]]
    @responses << [200, [error_event("SHUTTING_DOWN", "UNAVAILABLE", 503)]]
    @responses << [200, [transition("a", "SNAPSHOT", "COMPLETE", terminal: true), END_EVENT]]

    assert_equal [false, true], @client.watch_jobs("a").map(&:terminal?)
    assert_equal 3, requests.size
  end

  def test_a_connection_dropped_after_an_item_reopens
    @responses << [200, [transition("a", "SNAPSHOT", "PENDING")]]
    @responses << [200, [transition("a", "SNAPSHOT", "CANCELLED", terminal: true), END_EVENT]]

    assert_equal %i[pending cancelled], @client.watch_jobs("a").map(&:status)
  end

  def test_before_any_item_every_failure_raises
    @responses << [429, { "error" => status("WATCH_LIMIT", "RESOURCE_EXHAUSTED", 429, "cap" => "4") }]
    error = assert_raises(FlexiQ::RPCError) { @client.watch_jobs("a").to_a }

    assert_equal 4, error.watch_limit
    @responses.replace([[200, []]])
    assert_raises(FlexiQ::TransportError) { @client.watch_jobs("a").to_a }
    assert_equal 2, requests.size
  end

  def test_an_error_event_that_does_not_clear_raises
    @responses << [200, [transition("a", "SNAPSHOT", "PENDING"), error_event("INTERNAL", "INTERNAL", 500)]]
    error = assert_raises(FlexiQ::RPCError) { @client.watch_jobs("a").to_a }

    assert_equal "INTERNAL", error.reason
  end

  def test_breaking_out_ends_the_watch_and_the_callers_errors_are_its_own
    @responses << [200, [transition("a", "SNAPSHOT", "PENDING"), transition("a", "STARTED", "RUNNING")]]

    assert_equal :snapshot, @client.watch_jobs("a").first.kind
    assert_raises(FlexiQ::TransportError) { @client.watch_jobs("a") { raise FlexiQ::TransportError, "mine" } }
    # An IOError passes through the transport's network rescue; it must come back unwrapped.
    error = assert_raises(IOError) { @client.watch_jobs("a") { raise IOError, "disk full" } }

    assert_equal "disk full", error.message
    assert_equal 3, requests.size
  end

  def test_an_empty_id_list_is_refused_before_any_call
    assert_raises(ArgumentError) { @client.watch_jobs([]) { nil } }
  end

  def test_a_queue_watch_keeps_every_cursor_and_resumes_from_the_last
    @responses << [200, [checkpoint("c0"), transition("a", "ENQUEUED", "PENDING", cursor: "c1"),
                         event({ "futureArm" => {}, "cursor" => "c2" }, id: "c2")]]
    @responses << [200, [transition("b", "ENQUEUED", "PENDING", cursor: "c3")]]
    items = @client.watch_queue("orders").take(4)

    assert_equal([FlexiQ::WatchCheckpoint.new(cursor: "c0"), "c1", FlexiQ::WatchCheckpoint.new(cursor: "c2"), "c3"],
                 items.map { |item| item.is_a?(FlexiQ::JobTransition) ? item.cursor : item })
    assert_equal([[%w[queue orders]], [%w[queue orders], %w[resumeCursor c2]]],
                 requests.map { |request| query(request) })
  end

  def test_an_expired_cursor_shows_the_gap_and_starts_again_from_now
    @responses << [400, { "error" => status("WATCH_CURSOR_EXPIRED", "FAILED_PRECONDITION", 400) }]
    @responses << [200, [checkpoint("c9")]]
    items = @client.watch_queue(resume_cursor: "old").take(2)

    assert_equal [FlexiQ::WatchGap.new(lost_cursor: "old"), FlexiQ::WatchCheckpoint.new(cursor: "c9")], items
    assert_equal([[["queue", ""], %w[resumeCursor old]], [["queue", ""]]], requests.map { |request| query(request) })
  end
end
