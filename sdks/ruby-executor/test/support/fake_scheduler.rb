# frozen_string_literal: true

# An in-process ExecutorService on a loopback port, scripted by the test.
#
# Each Attach becomes a FakeAttach the test drives: read what the client sent, send frames back,
# end the stream cleanly (a rotation) or with a status.
class FakeScheduler < FlexiQ::Executor::V1::ExecutorService::Service
  SESSION = "\x00session\xFF".b
  SESSION_KEY = FlexiQ::Executor::Session::SESSION_METADATA_KEY

  attr_reader :attaches, :heartbeats, :address

  def initialize
    super
    @attaches = Queue.new
    @heartbeats = Queue.new
    @server = GRPC::RpcServer.new(pool_size: 16)
    @address = "127.0.0.1:#{@server.add_http2_port("127.0.0.1:0", :this_port_is_insecure)}"
    @server.handle(self)
    @thread = Thread.new { @server.run }
    @server.wait_till_running(5)
  end

  def attach(requests, call)
    FakeAttach.new(requests, call).tap { @attaches << _1 }.responses
  end

  def heartbeat(request, call)
    @heartbeats << [request, call.metadata]
    V1::HeartbeatResponse.new
  end

  def next_attach(timeout: 5) = @attaches.pop(timeout: timeout) || raise("no attach within #{timeout}s")

  def stop
    @server.stop
    @thread.join(5)
  end
end

# One attached stream, server side.
class FakeAttach
  END_STREAM = Object.new.freeze

  attr_reader :metadata

  def initialize(requests, call)
    @metadata = call.metadata
    @inbox = Queue.new
    @outbox = Queue.new
    call.merge_metadata_to_send(FakeScheduler::SESSION_KEY => FakeScheduler::SESSION)
    Thread.new do
      requests.each { @inbox << _1 }
      @inbox << :half_closed
      # A real scheduler ends the stream once the executor half-closes; so does this one.
      finish
    rescue StandardError
      @inbox << :broken
    end
  end

  def responses
    Enumerator.new do |out|
      while (frame = @outbox.pop) != END_STREAM
        raise frame if frame.is_a?(Exception)

        out << frame
      end
    end
  end

  # The next frame the client sent, or :half_closed once it closed its side.
  def receive(timeout: 5) = @inbox.pop(timeout: timeout) || raise("nothing from the client within #{timeout}s")

  # Skips frames until one with the given arm arrives.
  def receive_arm(arm, timeout: 5)
    loop do
      frame = receive(timeout: timeout)
      raise "the client half-closed before sending #{arm}" if frame == :half_closed
      return frame if frame.frame == arm
    end
  end

  # Skips frames until the client half-closes its side of the stream.
  def await_half_close(timeout: 5)
    loop { return if receive(timeout: timeout) == :half_closed }
  end

  def send_frame(**arm) = @outbox << V1::AttachResponse.new(**arm)

  def ack(capabilities: %w[side_channel lease steps], protocol_version: 1)
    send_frame(hello_ack: V1::HelloAckFrame.new(scheduler_id: "fake", protocol_version: protocol_version,
                                                capabilities: capabilities))
  end

  def dispatch(**fields) = send_frame(job: job_frame(**fields))

  def finish = @outbox << END_STREAM

  def fail_with(code, details) = @outbox << GRPC::BadStatus.new_status_exception(code, details)
end
