# frozen_string_literal: true

require_relative "fake_scheduler"

# A Worker running on its own thread against a FakeScheduler, torn down after every test.
module WorkerHarness
  def setup
    @scheduler = FakeScheduler.new
    @handlers = { "add" => ->(job) { job.args.sum } }
  end

  def teardown
    @worker&.stop
    begin
      @runner&.join(10)
    rescue FlexiQ::Error
      nil # the test asserted on it already
    end
    @scheduler.stop
  end

  private

  def start(**)
    @worker = FlexiQ::Executor::Worker.new(@scheduler.address, token: "token", insecure: true, logger: quiet_logger,
                                                               slots: 2, heartbeat_interval: 0.1, backoff_min: 0.01,
                                                               backoff_max: 0.05, **)
    @handlers.each { |name, handler| @worker.handle(name, handler) }
    @runner = Thread.new { @worker.run }
    @runner.report_on_exception = false
  end

  # Starts a worker and answers its hello; returns the attached stream.
  def attached(capabilities: %w[side_channel lease steps], **)
    start(**)
    attach = @scheduler.next_attach
    attach.receive
    attach.ack(capabilities: capabilities)
    attach
  end

  def await_heartbeat(timeout: 5)
    loop do
      request, = @scheduler.heartbeats.pop(timeout: timeout) || raise("no matching heartbeat within #{timeout}s")
      return request if yield(request)
    end
  end
end
