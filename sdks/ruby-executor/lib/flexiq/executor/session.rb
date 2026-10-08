# frozen_string_literal: true

require "English"

module FlexiQ
  module Executor
    # One attach: a bidirectional stream from `hello` to its end, and the jobs it dispatched.
    #
    # `run` returns how the stream ended. A Worker reconnects after `:rotated` and `:error`, and
    # stops after `:shutdown` and `:stopped`.
    class Session
      SESSION_METADATA_KEY = "flexiq-attach-session-bin"
      # How long the writer gets to send what is queued once the session stops taking work.
      FLUSH_BUDGET = 5
      # How long a half-closed stream waits for the scheduler to end it before it is cancelled.
      CLOSE_GRACE = 5

      Ending = Data.define(:reason, :attached, :error)

      def initialize(config, stub, handlers, slots)
        @config = config
        @log = config.logger
        @stub = stub
        @handlers = handlers
        @slots = slots
        @mutex = Mutex.new
        @running = {}
        @warned = {}
        @acked = Queue.new
        @heartbeat_stop = Queue.new
        @done = Queue.new
        @outbox = Outbox.new
      end

      # Attaches and serves the stream until it ends. Never raises for a stream failure: the
      # failure comes back in the Ending.
      def run
        @slots.resume
        @outbox.push(hello)
        @op = @stub.attach(@outbox.frames(-> { @finished || !@op&.status.nil? }),
                           metadata: metadata, return_op: true)
        watch_handshake
        @op.execute.each { |response| receive(response) }
        ending(@reason || :rotated)
      rescue AttachError => e
        ending(@reason || :error, e)
      rescue GRPC::BadStatus => e
        ending(@reason || :error, @refusal || AttachError.from(e))
      ensure
        teardown
      end

      # Stops taking work, lets running jobs finish within the drain budget, flushes their
      # results and half-closes the stream. Idempotent; returns at once.
      def begin_drain(reason)
        @mutex.synchronize do
          return if @drainer

          @reason ||= reason
          @drainer = Thread.new { drain }
        end
      end

      private

      def metadata = { "authorization" => @config.bearer }

      def hello
        V1::AttachRequest.new(hello: V1::HelloFrame.new(
          executor_id: @config.id, sdk: @config.sdk, version: @config.version, tasks: @handlers.keys.sort,
          slots: @config.slots, protocol_version: PROTOCOL_VERSION, capabilities: CAPABILITIES
        ))
      end

      # A `Heartbeat` that overtakes `hello_ack` is read as the handshake and refuses the attach,
      # so nothing else is sent until the acknowledgement arrives, and only so long for it.
      def watch_handshake
        Thread.new do
          # nil is the timeout; true or false means the handshake already resolved.
          next unless @acked.pop(timeout: @config.handshake_timeout).nil?

          @refusal = AttachError.new(GRPC::Core::StatusCodes::DEADLINE_EXCEEDED,
                                     "no hello acknowledgement within #{@config.handshake_timeout}s",
                                     permanent: false)
          @op.cancel
        end
      end

      def receive(response)
        frame = response.frame
        return handshake(response.hello_ack) if frame == :hello_ack && !@attached

        unless @attached
          raise AttachError.new(GRPC::Core::StatusCodes::FAILED_PRECONDITION,
                                "the scheduler's first frame was not a hello acknowledgement", permanent: true)
        end

        case frame
        when :job then dispatch(response.job)
        when :cancel then cancel(response.cancel.job_id)
        when :shutdown then begin_drain(:shutdown)
        when :hello_ack then once(:hello_ack, "a second hello acknowledgement on an attached stream; ignoring it")
        when :job_steps, :step_ack then once(:steps, "a durable-step frame this client did not ask for; ignoring it")
        else once(:unknown, "the scheduler sent a frame this build does not recognise; ignoring it")
        end
      end

      def handshake(ack)
        unless ack.protocol_version == PROTOCOL_VERSION
          @acked << true
          raise AttachError.new(GRPC::Core::StatusCodes::FAILED_PRECONDITION,
                                "protocol version mismatch: we speak #{PROTOCOL_VERSION}, " \
                                "the scheduler speaks #{ack.protocol_version}", permanent: true)
        end

        @token = @op.metadata&.fetch(SESSION_METADATA_KEY, nil)
        @log.warn("the attach response carried no #{SESSION_METADATA_KEY}; heartbeats will be refused") unless @token
        @side_on = ack.capabilities.include?(CAP_SIDE_CHANNEL)
        @attached = true
        @acked << true
        @log.info("attached as #{@config.id} to #{ack.scheduler_id} (slots=#{@config.slots}, " \
                  "tasks=#{@handlers.size}, side_channel=#{@side_on}, lease=#{ack.capabilities.include?(CAP_LEASE)})")
        @heartbeat = Thread.new { heartbeat_loop }
      end

      def dispatch(frame)
        job = Job.new(frame, side: @side_on ? @outbox : nil)
        handler = @handlers[job.task_name]
        unless handler
          return refuse(job, "TaskNotRegistered", "task not registered: #{job.task_name}", should_retry: false)
        end
        unless @slots.acquire
          return refuse(job, "NoCapacity", "executor did not run '#{job.task_name}': no free slot", should_retry: true)
        end

        # Booked before the thread starts: a `cancel` can follow its `job` frame at once.
        @mutex.synchronize { @running[job.id] = job }
        Thread.new { execute(job, handler) }
      end

      def refuse(job, errtype, message, should_retry:)
        @outbox.push(Outcome.failure(job, TaskError.encode(errtype, message), should_retry: should_retry))
      end

      def execute(job, handler)
        settled = false
        started = Clock.now
        value = error = nil
        begin
          value = handler.call(job)
        rescue StandardError, ScriptError => e
          error = e
        end
        settle(job, Outcome.new(value: value, error: error, wall: Clock.now - started,
                                timed_out: job.timed_out?, cancelled: job.cancelled?).frame(job))
        settled = true
      ensure
        # Exactly one settling frame per job, even for a thread that was killed or raised past
        # StandardError: a job never settled is a job the scheduler waits on until its timeout.
        unless settled
          message = "the handler ended without returning: #{$ERROR_INFO&.class || "thread killed"}"
          settle(job, Outcome.failure(job, TaskError.encode("HandlerAborted", message), should_retry: true))
        end
        @mutex.synchronize { @running.delete(job.id) }
        @slots.release
      end

      def settle(job, frame)
        @outbox.promote(job.id)
        @outbox.push(frame)
      end

      def cancel(job_id)
        @mutex.synchronize { @running[job_id] }&.request_cancel!
      end

      def heartbeat_loop
        heartbeat(@slots.available) until @heartbeat_stop.pop(timeout: @config.heartbeat_interval)
      end

      # Unary and off the stream, so it carries the session token rather than the executor id:
      # an id is a name this executor picked, and could be another executor's.
      def heartbeat(free_slots)
        return unless @token

        # The interval bounds the call, but never below a second, so a short interval in a test
        # cannot make every heartbeat time out.
        deadline = Time.now + [@config.heartbeat_interval, 1].max
        @stub.heartbeat(V1::HeartbeatRequest.new(session: @token, free_slots: free_slots),
                        metadata: metadata, deadline: deadline)
      rescue GRPC::BadStatus => e
        @log.debug("heartbeat failed: #{e.message}")
      end

      def drain
        @slots.drain
        # Zero capacity first, so the scheduler stops matching work to this stream.
        heartbeat(0) if @attached
        unless @slots.await_idle(@config.shutdown_drain)
          @log.warn("#{@slots.in_flight} job(s) still running after the #{@config.shutdown_drain}s drain")
        end
        @outbox.close
        @log.warn("gave up flushing results after #{FLUSH_BUDGET}s") unless @outbox.await_drained(FLUSH_BUDGET)
        # The scheduler ends the stream once it reads the half-close; cancel only if it does not.
        @op&.cancel unless @done.pop(timeout: CLOSE_GRACE)
      end

      def teardown
        @done << true
        @drainer&.join
        @finished = true
        @outbox.close
        @heartbeat_stop << true
        @heartbeat&.join
        @acked << false
        @mutex.synchronize { @running.each_value(&:request_cancel!) }
        @op&.cancel
      end

      def ending(reason, error = nil) = Ending.new(reason: reason, attached: @attached == true, error: error)

      def once(key, message)
        return if @warned[key]

        @warned[key] = true
        @log.warn(message)
      end
    end
  end
end
