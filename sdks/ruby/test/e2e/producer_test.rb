# frozen_string_literal: true

require_relative "../test_helper"
require_relative "support/server"
require "securerandom"

# The producer door end to end, against a real flexiq-server over its JSON door.
class ProducerE2ETest < Minitest::Test
  QUEUE = LiveServer::UNPOLLED_QUEUE

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

  def test_an_unknown_job_is_job_not_found
    error = assert_raises(FlexiQ::RPCError) { client.get_job(SecureRandom.uuid) }

    assert_equal [FlexiQ::Reason::JOB_NOT_FOUND, "NOT_FOUND", 404], [error.reason, error.code, error.http_status]
  end

  def test_a_bad_token_is_unauthenticated
    error = assert_raises(FlexiQ::RPCError) { client("fqt_0000000000000000.nope").queue_stats }

    assert_equal FlexiQ::Reason::UNAUTHENTICATED, error.reason
  end

  def test_a_token_without_produce_is_scope_denied
    token = server.mint("ruby-e2e-executor", "execute")
    error = assert_raises(FlexiQ::RPCError) { client(token).enqueue("nope", queue: QUEUE) }

    assert_equal [FlexiQ::Reason::SCOPE_DENIED, "produce"], [error.reason, error.scope]
  end
end
