# frozen_string_literal: true

module FlexiQ
  module Executor
    # Attaches to a flexiq-server's executor door and runs the jobs it dispatches.
    #
    #   worker = FlexiQ::Executor::Worker.new("queue.internal:50051", token: ENV.fetch("FLEXIQ_EXECUTE_TOKEN"))
    #   worker.handle("billing.charge") { |job| Receipt.for(job.args.first) }
    #   trap("TERM") { worker.stop }
    #   worker.run
    class Worker
      # Options (all keywords, all but `token:` optional):
      #
      # token::              bearer token carrying the `execute` scope
      # id::                 executor id, unique among attached executors; host and pid by default
      # slots::              jobs run at once; the processor count by default
      # insecure::           plaintext gRPC, for loopback and tests only
      # credentials::        a GRPC::Core::ChannelCredentials; the system roots by default
      # max_message_bytes::  68 MiB by default, the door's own ceiling
      # user_agent::         prepended to this gem's own
      # handshake_timeout::  seconds to wait for `hello_ack` (10)
      # heartbeat_interval:: seconds between heartbeats (5)
      # shutdown_drain::     seconds running jobs get to finish on a drain (30)
      # backoff_min, backoff_max:: reconnect delay bounds in seconds (0.25, 30)
      # logger::             a Logger; $stderr at INFO by default
      def initialize(target, **)
        @config = Config.new(**)
        @stub = V1::ExecutorService::Stub.new(target, @config.channel_credentials,
                                              channel_args: @config.channel_args)
        @handlers = {}
        @mutex = Mutex.new
        @wakeup = Queue.new
        @started = false
        @stopping = false
      end

      # Registers the handler for one task name: a block, or anything responding to `call`. It is
      # given a Job; what it returns is the result, what it raises is the failure.
      def handle(task_name, callable = nil, &block)
        handler = callable || block
        raise ArgumentError, "task name must not be empty" if task_name.to_s.empty?
        raise ArgumentError, "handler for #{task_name.inspect} must respond to call" unless handler.respond_to?(:call)

        @mutex.synchronize do
          raise FlexiQ::Error, "cannot register #{task_name.inspect} after run has started" if @started
          raise ArgumentError, "task #{task_name.inspect} is already registered" if @handlers.key?(task_name.to_s)

          @handlers[task_name.to_s] = handler
        end
        self
      end

      # Attaches and serves until the scheduler sends `shutdown` or `stop` is called. A stream
      # the scheduler ends is a rotation, not a failure: it reconnects. Raises AttachError for a
      # refusal that reconnecting cannot fix.
      def run
        handlers = begin_run
        slots = Slots.new(@config.slots)
        backoff = Backoff.new(@config.backoff_min, @config.backoff_max)

        while (session = next_session(handlers, slots))
          ending = session.run
          backoff.reset if ending.attached
          return if finished?(ending)
          return if retry_after?(ending) && pause(backoff.next_delay)
        end
      end

      # Drains and ends `run`: no new work, running jobs get `shutdown_drain` seconds, their
      # results are flushed. Returns at once, and is safe to call from a signal handler.
      def stop
        Thread.new do
          session = @mutex.synchronize do
            @stopping = true
            @session
          end
          @wakeup << true
          session&.begin_drain(:stopped)
        end
        nil
      end

      private

      def begin_run
        @mutex.synchronize do
          raise FlexiQ::Error, "run has already been called on this worker" if @started
          if @handlers.empty?
            raise FlexiQ::Error, "no handlers registered: an executor advertising no tasks is sent no work"
          end

          @started = true
          @handlers.dup.freeze
        end
      end

      def next_session(handlers, slots)
        @mutex.synchronize do
          @session = @stopping ? nil : Session.new(@config, @stub, handlers, slots)
        end
      end

      def finished?(ending)
        case ending.reason
        when :shutdown
          @config.logger.info("the scheduler sent shutdown; stopping")
          true
        when :stopped then true
        else false
        end
      end

      # True when the next attach should wait first. A rotation reconnects at once; a failure,
      # or a stream that ended before it ever attached, backs off.
      def retry_after?(ending)
        if ending.reason == :rotated && ending.attached
          @config.logger.info("the scheduler ended the stream; reconnecting")
          return false
        end
        error = ending.error
        raise error if error&.permanent?

        @config.logger.warn("attach failed; retrying: #{error&.message || "the stream ended before hello_ack"}")
        true
      end

      # Sleeps `delay` seconds; true when `stop` cut the wait short.
      def pause(delay)
        @wakeup.pop(timeout: delay)
        @mutex.synchronize { @stopping }
      end
    end
  end
end
