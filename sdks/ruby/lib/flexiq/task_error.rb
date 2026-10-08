# frozen_string_literal: true

require "json"

module FlexiQ
  # Why a job failed, read from `Job#error`.
  #
  # The canonical form is `{"errtype", "message", "traceback"}` JSON. Anything else — a timeout,
  # an expiry, a worker-death recovery, a legacy error — is plain text by design, and comes back
  # with `structured` false and the raw text as the message rather than as an exception.
  TaskError = Data.define(:errtype, :message, :traceback, :structured, :raw) do
    # The rule turns on `message` alone: a document with a string message is canonical, and a
    # missing or mistyped sibling falls back to its default rather than losing the message.
    def self.parse(raw)
      fields = JSON.parse(raw)
      message = fields["message"] if fields.is_a?(Hash)
      return unstructured(raw) unless message.is_a?(String)

      errtype = fields["errtype"]
      new(
        errtype: errtype.is_a?(String) ? errtype : "Error",
        message: message,
        traceback: frames(fields["traceback"]),
        structured: true,
        raw: raw
      )
    rescue JSON::ParserError
      unstructured(raw)
    end

    # Writes the canonical document. Key order is part of the cross-SDK shape, and a Hash
    # literal keeps it.
    def self.encode(errtype, message, traceback = [])
      JSON.generate({ "errtype" => errtype.to_s, "message" => message.to_s, "traceback" => traceback.map(&:to_s) })
    end

    def self.unstructured(raw)
      new(errtype: nil, message: raw, traceback: [], structured: false, raw: raw)
    end

    def self.frames(value)
      value.is_a?(Array) ? value.grep(String) : []
    end
    private_class_method :unstructured, :frames

    alias_method :structured?, :structured

    def to_s = errtype ? "#{errtype}: #{message}" : message
  end
end
