# frozen_string_literal: true

module FlexiQ
  # Root of every error this gem raises, so one `rescue FlexiQ::Error` covers them all.
  class Error < StandardError; end

  # The client was built with settings it cannot honour (no token, plaintext without opt-in, …).
  class ConfigurationError < Error; end

  # A value could not be written as, or read from, the CBOR payload envelope.
  class CodecError < Error; end

  # The request never produced a FlexiQ answer: DNS, connect, TLS, timeout, or a non-FlexiQ body.
  #
  # On a write this does not mean the write failed — the connection may have dropped after
  # the server committed it. Retry a write only with `unique_key` set, reusing the same value.
  class TransportError < Error; end
end
