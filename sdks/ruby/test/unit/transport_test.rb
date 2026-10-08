# frozen_string_literal: true

require_relative "../test_helper"

class TransportTest < Minitest::Test
  def build(url, **) = FlexiQ::Transport.new(url, token: "t", **)

  def test_plaintext_needs_an_explicit_opt_in
    error = assert_raises(FlexiQ::ConfigurationError) { build("http://localhost:50051") }
    assert_match(/insecure: true/, error.message)
    build("http://localhost:50051", insecure: true)
  end

  def test_https_verifies_the_peer
    http = build("https://flexiq.internal").instance_variable_get(:@http)

    assert_predicate http, :use_ssl?
    assert_equal OpenSSL::SSL::VERIFY_PEER, http.verify_mode
  end

  def test_bad_urls_and_tokens_are_refused
    ["flexiq.internal", "ftp://host", "https://host/?q=1", "::"].each do |url|
      assert_raises(FlexiQ::ConfigurationError, url) { build(url) }
    end
    assert_raises(FlexiQ::ConfigurationError) { FlexiQ::Transport.new("https://h", token: "") }
    assert_raises(FlexiQ::ConfigurationError) { FlexiQ::Transport.new("https://h", token: "a\r\nX-Evil: 1") }
  end

  def test_unknown_tls_options_are_refused
    assert_raises(FlexiQ::ConfigurationError) { build("https://h", tls: { verify: false }) }
  end
end
