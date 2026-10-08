# frozen_string_literal: true

require_relative "../test_helper"

class ConfigTest < Minitest::Test
  def test_defaults_raise_the_message_limit_both_ways
    args = FlexiQ::Executor::Config.new(token: "t").channel_args

    assert_equal [68 << 20, 68 << 20],
                 args.values_at("grpc.max_send_message_length", "grpc.max_receive_message_length")
    assert_match %r{\Aflexiq-ruby-executor/}, args["grpc.primary_user_agent"]
  end

  def test_tls_unless_opted_out
    assert_instance_of GRPC::Core::ChannelCredentials, FlexiQ::Executor::Config.new(token: "t").channel_credentials
    assert_equal :this_channel_is_insecure, FlexiQ::Executor::Config.new(token: "t", insecure: true).channel_credentials
  end

  def test_invalid_settings_are_refused
    [{ token: "" }, { token: "a\nb" }, { token: nil }, { token: "t", id: "" }, { token: "t", slots: 0 },
     { token: "t", slots: 1.5 }, { token: "t", heartbeat_interval: 0 }, { token: "t", backoff_min: 2, backoff_max: 1 },
     { token: "t", max_message_bytes: 0 }].each do |settings|
      assert_raises(FlexiQ::ConfigurationError, settings.inspect) { FlexiQ::Executor::Config.new(**settings) }
    end
  end
end
