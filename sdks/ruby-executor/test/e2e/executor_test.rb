# frozen_string_literal: true

require_relative "../test_helper"
require_relative "support/server"

# The executor door end to end: jobs enqueued through the producer gem over the JSON door, run
# and settled by a Worker attached over gRPC, read back through the producer gem.
class ExecutorE2ETest < Minitest::Test
  QUEUE = LiveServer::QUEUE
  BUDGET = 30
  # Past gRPC's 4 MiB default, so a client that never raised its limit fails here.
  LARGE = 5 * 1024 * 1024

  class << self
    # One server and one attached worker for the suite; stopped when Minitest exits.
    def live
      @live ||= begin
        binary = LiveServer.binary
        raise "no flexiq-server; build one with:\n  #{LiveServer::BUILD_COMMAND}" unless binary

        server = LiveServer.new(binary)
        begin
          server.start
          producer = FlexiQ::Client.new(server.url, token: server.mint("producer", "produce", "read"), insecure: true)
          server.await_ready(producer)
          worker, runner = attach(server)
        rescue StandardError
          server.stop
          raise
        end
        Minitest.after_run do
          worker.stop
          runner.join(BUDGET)
          server.stop
        end
        { server: server, producer: producer }
      end
    end

    private

    def attach(server)
      worker = FlexiQ::Executor::Worker.new(server.address, token: server.mint("executor", "execute"), insecure: true,
                                                            id: "ruby-e2e", slots: 4, logger: quiet_logger)
      register(worker)
      runner = Thread.new { worker.run }
      runner.report_on_exception = false
      [worker, runner]
    end

    def register(worker)
      worker.handle("sum") do |job|
        job.progress(50)
        job.log("info", "summing", extra: { "count" => job.args.length })
        job.args.sum
      end
      worker.handle("echo_kwargs", &:kwargs)
      worker.handle("boom") { |_job| raise ArgumentError, "bad value 42" }
      worker.handle("fatal") { |_job| raise FlexiQ::Executor::Fatal, "no point retrying" }
      worker.handle("large") { |job| "x" * job.args.first }
      worker.handle("until_cancelled") do |job|
        loop do
          job.check!
          sleep 0.02
        end
      end
    end
  end

  def producer = self.class.live[:producer]

  def test_a_job_runs_and_its_result_reads_back
    job = finished(producer.enqueue("sum", args: [1, 2, 3], queue: QUEUE))

    assert_equal :complete, job.status
    assert_equal 6, FlexiQ::Payload.decode_result(job.result)
    # The side channel reached storage, lease and all: a frame missing its lease is dropped.
    assert_equal 50, job.progress
  end

  def test_kwargs_arrive_as_sent
    job = finished(producer.enqueue("echo_kwargs", kwargs: { "a" => [1, "two"], "b" => nil }, queue: QUEUE))

    assert_equal({ "a" => [1, "two"], "b" => nil }, FlexiQ::Payload.decode_result(job.result))
  end

  def test_a_raised_error_lands_in_the_canonical_shape
    job = finished(producer.enqueue("boom", queue: QUEUE, max_retries: 0))
    error = FlexiQ::TaskError.parse(job.error)

    assert_equal :dead, job.status
    assert_predicate error, :structured?
    assert_equal ["ArgumentError", "bad value 42"], [error.errtype, error.message]
    refute_empty error.traceback
  end

  def test_fatal_is_not_retried
    job = finished(producer.enqueue("fatal", queue: QUEUE, max_retries: 3))

    assert_equal [:dead, 0], [job.status, job.retry_count]
    assert_equal "no point retrying", FlexiQ::TaskError.parse(job.error).message
  end

  def test_a_result_past_four_mebibytes_is_delivered
    job = finished(producer.enqueue("large", args: [LARGE], queue: QUEUE))

    assert_equal :complete, job.status
    assert_equal LARGE, FlexiQ::Payload.decode_result(job.result).bytesize
  end

  def test_a_cancel_reaches_a_running_handler
    id = producer.enqueue("until_cancelled", queue: QUEUE).job.id
    await { producer.get_job(id).status == :running }
    producer.cancel_job(id)

    assert_equal :cancelled, finished_id(id).status
  end

  private

  def finished(result) = finished_id(result.job.id)

  def finished_id(id)
    producer.wait(id, timeout: BUDGET)
    producer.get_job(id, include_result: true)
  end

  def await
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + BUDGET
    until yield
      flunk "condition not met within #{BUDGET}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.05
    end
  end
end
