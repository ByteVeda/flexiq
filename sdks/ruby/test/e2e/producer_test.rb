# frozen_string_literal: true

require_relative "../test_helper"
require_relative "support/server"
require "securerandom"
require "timeout"

# The producer door end to end, against a real flexiq-server over its JSON door.
class ProducerE2ETest < Minitest::Test
  QUEUE = LiveServer::UNPOLLED_QUEUE
  WATCH_BUDGET = 30

  class << self
    # One server for the suite; started on first use, stopped when Minitest exits.
    def live
      @live ||= begin
        binary = LiveServer.binary
        raise "no flexiq-server; build one with:\n  #{LiveServer::BUILD_COMMAND}" unless binary

        server = LiveServer.new(binary)
        begin
          server.start
          token = server.mint("ruby-e2e-producer", "produce")
          server.await_ready(FlexiQ::Client.new(server.url, token: token, insecure: true))
        rescue StandardError
          # A server that never became usable still owns a process and a database file.
          server.stop
          raise
        end
        Minitest.after_run { server.stop }
        { server: server, token: token }
      end
    end
  end

  def server = self.class.live[:server]

  def client(token = self.class.live[:token]) = FlexiQ::Client.new(server.url, token: token, insecure: true)

  # A watch that never ends would hang the suite; fail it instead.
  def bounded(&) = Timeout.timeout(WATCH_BUDGET, &)

  def test_an_enqueued_job_reads_back_with_its_exact_payload
    args = [{ "order_id" => "ord-0001", "amount_cents" => 1000 }]
    result = client.enqueue("orders.charge", args: args, kwargs: { "retry" => true }, queue: QUEUE, priority: 3)
    job = client.get_job(result.job.id, include_payload: true)

    assert_equal ["orders.charge", QUEUE, :pending, 3], [job.task_name, job.queue, job.status, job.priority]
    assert_equal LiveServer::NAMESPACE, job.namespace
    assert_equal FlexiQ::Payload.encode_call(args, { "retry" => true }), job.payload
    assert_equal [args, { "retry" => true }], job.decode_payload
    refute_predicate job, :terminal?
  end

  def test_a_unique_key_dedupes_against_the_live_job
    key = "ruby-e2e-#{SecureRandom.hex(4)}"
    first = client.enqueue("dedupe", queue: QUEUE, unique_key: key)
    second = client.enqueue("dedupe", queue: QUEUE, unique_key: key)

    refute_predicate first, :deduplicated?
    assert_predicate second, :deduplicated?
    assert_equal first.job.id, second.job.id
  end

  def test_a_batch_enqueues_every_item
    results = client.enqueue_batch([
                                     { task_name: "batch.a", args: [1], options: { queue: QUEUE } },
                                     FlexiQ::EnqueueRequest.new(task_name: "batch.b", options: { queue: QUEUE })
                                   ])

    assert_equal [true, true], results.map(&:enqueued?)
    assert_equal(%w[batch.a batch.b], results.map { |item| item.enqueued.job.task_name })
  end

  def test_cancelling_a_pending_job_finishes_it
    job = client.enqueue("cancel.me", queue: QUEUE).job
    cancelled = client.cancel_job(job.id)

    assert_equal :cancelled, cancelled.status
    assert_predicate cancelled, :terminal?
    assert_equal :cancelled, client.cancel_job(job.id).status
  end

  def test_queue_stats_count_this_queue_and_the_namespace
    before = client.queue_stats(QUEUE).pending
    client.enqueue("count.me", queue: QUEUE)

    assert_equal before + 1, client.queue_stats(QUEUE).pending
    assert_operator client.queue_stats.pending, :>=, before + 1
  end

  def test_a_listing_pages_past_page_size
    task = "list.#{SecureRandom.hex(4)}"
    ids = Array.new(3) { client.enqueue(task, queue: QUEUE).job.id }

    first = client.list_jobs(task_name: task, status: :pending, page_size: 2)
    last = client.list_jobs(task_name: task, status: :pending, page_size: 2, page_token: first.next_page_token)

    assert_equal [2, 1], [first.jobs.length, last.jobs.length]
    refute_predicate first, :last_page?
    assert_predicate last, :last_page?
    assert_equal ids.sort, (first.jobs + last.jobs).map(&:id).sort
    assert_equal ids.sort, client.each_job(task_name: task, page_size: 2).map(&:id).sort
    assert_nil first.jobs.first.payload
  end

  def test_a_narrowed_grant_must_name_a_queue_it_reaches
    token = server.mint("ruby-e2e-reader", "read:queue=#{QUEUE}")
    error = assert_raises(FlexiQ::RPCError) { client(token).list_jobs }

    assert_equal FlexiQ::Reason::SCOPE_DENIED, error.reason
    assert_kind_of FlexiQ::JobPage, client(token).list_jobs(queue: QUEUE)
  end

  def test_a_forged_page_token_is_invalid_request
    error = assert_raises(FlexiQ::RPCError) { client.list_jobs(page_token: "not-a-token") }

    assert_equal [FlexiQ::Reason::INVALID_REQUEST, "INVALID_ARGUMENT"], [error.reason, error.code]
  end

  def test_an_unknown_job_is_job_not_found
    error = assert_raises(FlexiQ::RPCError) { client.get_job(SecureRandom.uuid) }

    assert_equal [FlexiQ::Reason::JOB_NOT_FOUND, "NOT_FOUND", 404], [error.reason, error.code, error.http_status]
  end

  def test_a_bad_token_is_unauthenticated
    error = assert_raises(FlexiQ::RPCError) { client("fqt_0000000000000000.nope").queue_stats }

    assert_equal FlexiQ::Reason::UNAUTHENTICATED, error.reason
  end

  def test_an_id_watch_follows_a_job_until_it_is_cancelled
    watcher = client
    job = watcher.enqueue("watch.me", queue: QUEUE).job
    missing = SecureRandom.uuid
    items = []
    # One client for both: the watch holds a connection of its own, so the cancel is not blocked.
    bounded do
      watcher.watch_jobs([job.id, missing]) do |item|
        items << item
        watcher.cancel_job(job.id) if items.one?
      end
    end
    transitions = items.grep(FlexiQ::JobTransition)

    assert_includes items, FlexiQ::JobNotFound.new(job_id: missing)
    assert_equal [:snapshot, :pending, false], transitions.first.to_h.values_at(:kind, :status, :terminal)
    assert_equal [:cancelled, true], transitions.last.to_h.values_at(:status, :terminal)
  end

  def test_wait_returns_the_finished_job_and_times_out_on_a_running_one
    pending = client.enqueue("wait.me", queue: QUEUE).job
    error = assert_raises(FlexiQ::WaitTimeoutError) { bounded { client.wait(pending.id, timeout: 0.5) } }

    assert_match(/0.5s/, error.message)
    client.cancel_job(pending.id)

    assert_equal :cancelled, bounded { client.wait(pending.id, timeout: 10) }.status
    assert_raises(FlexiQ::RPCError) { bounded { client.wait(SecureRandom.uuid, timeout: 10) } }
  end

  def test_a_queue_watch_resumes_from_any_cursor_and_shows_an_expired_one_as_a_gap
    queue = "watched-#{SecureRandom.hex(4)}"
    opened = nil
    live = bounded do
      client.watch_queue(queue).each do |item|
        break item if item.is_a?(FlexiQ::JobTransition)

        opened = item
        client.enqueue("watch.queue", queue: queue)
      end
    end

    assert_equal [:enqueued, queue], [live.kind, live.queue]
    refute_nil live.cursor
    replayed = bounded { client.watch_queue(queue, resume_cursor: opened.cursor).take(2) }

    assert_equal [FlexiQ::WatchCheckpoint, live], [replayed[0].class, replayed[1]]
    # Well-formed, but numbered by no process this server ran.
    forged = "AAAAAAAAAAAAAAAAAAAAAA"
    gap, checkpoint = bounded { client.watch_queue(queue, resume_cursor: forged).take(2) }

    assert_equal FlexiQ::WatchGap.new(lost_cursor: forged), gap
    assert_kind_of FlexiQ::WatchCheckpoint, checkpoint
  end

  def two_node_graph(queue: QUEUE, **ship)
    FlexiQ::WorkflowGraph.new(
      nodes: [
        { name: "charge", task_name: "wf.charge", args: [{ "order_id" => "o-1" }], queue: queue },
        { name: "ship", task_name: "wf.ship", queue: queue, condition: :on_success, **ship }
      ],
      edges: [%w[charge ship]]
    )
  end

  def workflow_name = "ruby-e2e-#{SecureRandom.hex(4)}"

  def test_a_static_workflow_enqueues_each_node_and_reads_back
    run_id = client.submit_workflow(workflow_name, two_node_graph, params_json: '{"order":"o-1"}')
    view = client.get_workflow_run(run_id)

    assert_equal run_id, view.run.id
    # Nothing polls QUEUE, so the run stays where submission left it.
    assert_equal :running, view.run.state
    refute_predicate view.run, :terminal?
    assert_equal %w[charge ship], view.nodes.map(&:name).sort
    charge = client.get_job(view.node("charge").job_id, include_payload: true)

    assert_equal ["wf.charge", QUEUE], [charge.task_name, charge.queue]
    assert_equal FlexiQ::Payload.encode_call([{ "order_id" => "o-1" }], {}), charge.payload
    assert_equal "wf.ship", client.get_job(view.node("ship").job_id).task_name
  end

  def test_a_dynamic_construct_is_refused_naming_the_node_and_the_field
    error = assert_raises(FlexiQ::RPCError) do
      client.submit_workflow(workflow_name, two_node_graph(gate: { timeout: 60 }))
    end

    assert_equal [FlexiQ::Reason::WORKFLOW_CONSTRUCT_UNSUPPORTED, "FAILED_PRECONDITION"], [error.reason, error.code]
    assert_equal({ node: "ship", field: "gate" }, error.workflow_construct)
    assert_match(/ship.*gate/, error.message)
  end

  def test_a_narrowed_grant_is_checked_on_every_node
    token = server.mint("ruby-e2e-wf-narrow", "produce:queue=#{QUEUE}")
    stray = FlexiQ::WorkflowNode.new(name: "ship", task_name: "wf.ship", queue: "elsewhere")
    graph = FlexiQ::WorkflowGraph.new(nodes: [two_node_graph.nodes.first, stray])
    error = assert_raises(FlexiQ::RPCError) { client(token).submit_workflow(workflow_name, graph) }

    assert_equal [FlexiQ::Reason::SCOPE_DENIED, "produce", "ship"], [error.reason, error.scope, error.node]
    run_id = client(token).submit_workflow(workflow_name, two_node_graph)

    assert_equal run_id, client(token).get_workflow_run(run_id).run.id
    outsider = server.mint("ruby-e2e-wf-outsider", "read:queue=elsewhere")
    hidden = assert_raises(FlexiQ::RPCError) { client(outsider).get_workflow_run(run_id) }

    assert_equal "NOT_FOUND", hidden.code
  end

  def test_a_token_without_produce_is_scope_denied
    token = server.mint("ruby-e2e-executor", "execute")
    error = assert_raises(FlexiQ::RPCError) { client(token).enqueue("nope", queue: QUEUE) }

    assert_equal [FlexiQ::Reason::SCOPE_DENIED, "produce"], [error.reason, error.scope]
  end
end
