# frozen_string_literal: true

require "etc"
require "logger"
require "socket"

module FlexiQ
  module Executor
    # The executor door's message ceiling: the 64 MiB a payload may be, plus 4 MiB of envelope.
    # Leaving gRPC's 4 MiB default attaches cleanly and fails on the first large job.
    MAX_MESSAGE_BYTES = 68 * 1024 * 1024

    # The attach protocol this client speaks; `hello_ack` must answer the same number.
    PROTOCOL_VERSION = 1

    # `hello.slots` is a uint32.
    MAX_SLOTS = 0xFFFF_FFFF

    CAP_SIDE_CHANNEL = "side_channel"
    CAP_LEASE = "lease"

    # Capabilities this client implements. `steps` is absent: durable steps fail rather than
    # degrade, so advertising them without an implementation would be a lie the scheduler acts on.
    CAPABILITIES = [CAP_SIDE_CHANNEL, CAP_LEASE].freeze

    # Settings for a Worker; see Worker.new for what each one means.
    Config = Data.define(
      :token, :id, :slots, :insecure, :credentials, :max_message_bytes, :user_agent, :sdk, :version,
      :handshake_timeout, :heartbeat_interval, :shutdown_drain, :backoff_min, :backoff_max, :logger
    ) do
      def self.default_id = "ruby-#{Socket.gethostname}-#{Process.pid}"

      def self.default_logger = Logger.new($stderr, progname: "flexiq-executor", level: Logger::INFO)

      def initialize(token:, id: Config.default_id, slots: Etc.nprocessors, insecure: false, credentials: nil,
                     max_message_bytes: MAX_MESSAGE_BYTES, user_agent: nil, sdk: "ruby", version: VERSION,
                     handshake_timeout: 10, heartbeat_interval: 5, shutdown_drain: 30,
                     backoff_min: 0.25, backoff_max: 30, logger: Config.default_logger)
        super
        validate!
      end

      def bearer = "Bearer #{token}"

      # What the channel is opened with: TLS unless the caller opted out, and the raised limit
      # in both directions.
      def channel_credentials
        return :this_channel_is_insecure if insecure

        credentials || GRPC::Core::ChannelCredentials.new
      end

      def channel_args
        args = { "grpc.max_send_message_length" => max_message_bytes,
                 "grpc.max_receive_message_length" => max_message_bytes }
        agent = [user_agent, "flexiq-ruby-executor/#{VERSION}"].compact.join(" ")
        args.merge("grpc.primary_user_agent" => agent)
      end

      private

      def validate!
        validate_token!
        raise ConfigurationError, "executor id must not be empty" if id.to_s.empty?
        unless slots.is_a?(Integer) && slots.between?(1, MAX_SLOTS)
          raise ConfigurationError, "slots must be between 1 and #{MAX_SLOTS}"
        end
        raise ConfigurationError, "max_message_bytes must be positive" unless max_message_bytes.to_i.positive?

        validate_durations!
      end

      def validate_token!
        raise ConfigurationError, "no token: every call to the executor door carries one" if token.to_s.empty?
        raise ConfigurationError, "token must be a String" unless token.is_a?(String)
        raise ConfigurationError, "token must not contain line breaks" if token.match?(/[\r\n]/)
      end

      def validate_durations!
        unless [handshake_timeout, heartbeat_interval, shutdown_drain].all? { |value| value.to_f.positive? }
          raise ConfigurationError, "handshake, heartbeat and drain durations must be positive"
        end
        return if backoff_min.to_f.positive? && backoff_max.to_f >= backoff_min.to_f

        raise ConfigurationError, "reconnect backoff must be positive and non-decreasing"
      end
    end
  end
end
