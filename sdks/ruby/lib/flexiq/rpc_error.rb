# frozen_string_literal: true

module FlexiQ
  # The server answered and refused: a `google.rpc.Status` with its `ErrorInfo`.
  #
  # Raised when a call fails, and also carried, unraised, by a batch item that did not land.
  # Branch on `#reason` (see FlexiQ::Reason); `#code` is a category, `#message` is prose.
  class RPCError < Error
    ERROR_INFO_TYPE = "type.googleapis.com/google.rpc.ErrorInfo"
    RETRY_INFO_TYPE = "type.googleapis.com/google.rpc.RetryInfo"

    # For a reply with no FlexiQ body (a proxy's 502, say): the code the HTTP status stands for.
    CODE_FOR_HTTP_STATUS = {
      400 => "INVALID_ARGUMENT", 401 => "UNAUTHENTICATED", 403 => "PERMISSION_DENIED",
      404 => "NOT_FOUND", 409 => "ABORTED", 429 => "RESOURCE_EXHAUSTED", 499 => "CANCELLED",
      501 => "UNIMPLEMENTED", 503 => "UNAVAILABLE", 504 => "DEADLINE_EXCEEDED"
    }.freeze
    private_constant :CODE_FOR_HTTP_STATUS

    # `google.rpc.Code` name, e.g. "RESOURCE_EXHAUSTED".
    attr_reader :code
    # HTTP status the JSON door answered with.
    attr_reader :http_status
    # ErrorInfo reason, or nil when the server sent none.
    attr_reader :reason
    # Per-reason facts; every value a String.
    attr_reader :metadata
    # Seconds the server asked the caller to wait (Rational), or nil.
    attr_reader :retry_after

    # Reads the `{"code", "status", "message", "details"}` object the JSON door renders.
    def self.from_status(status, http_status: nil)
      details = Array(status["details"])
      info = details.find { |d| d.is_a?(Hash) && d["@type"] == ERROR_INFO_TYPE && d["domain"] == Reason::DOMAIN }
      retry_info = details.find { |d| d.is_a?(Hash) && d["@type"] == RETRY_INFO_TYPE }
      new(
        status["message"].to_s,
        code: status["status"] || "UNKNOWN",
        http_status: Wire::Int64.load(status["code"]) || http_status,
        reason: info&.fetch("reason", nil),
        metadata: string_map(info&.fetch("metadata", nil)),
        retry_after: retry_info && Wire::Duration.load(retry_info["retryDelay"])
      )
    end

    # A refusal that arrived with no FlexiQ error body.
    def self.from_http(http_status, body)
      code = CODE_FOR_HTTP_STATUS.fetch(http_status) { http_status >= 500 ? "INTERNAL" : "UNKNOWN" }
      new("HTTP #{http_status} with no FlexiQ error body: #{body.to_s[0, 200]}", code: code, http_status: http_status)
    end

    def self.string_map(value)
      return {} unless value.is_a?(Hash)

      value.each_with_object({}) { |(k, v), out| out[k.to_s] = v.to_s if v.is_a?(String) }.freeze
    end
    private_class_method :string_map

    def initialize(message, code:, http_status: nil, reason: nil, metadata: {}, retry_after: nil)
      @code = code
      @http_status = http_status
      @reason = reason
      @metadata = metadata
      @retry_after = retry_after
      super(reason ? "#{reason} (#{code}): #{message}" : "#{code}: #{message}")
    end

    # Whether the condition clears on its own. Never a licence to resend a write blind:
    # resend only with `unique_key` set, reusing the same value.
    def retryable?
      return Reason::TRANSIENT.include?(reason) if reason

      %w[UNAVAILABLE DEADLINE_EXCEEDED].include?(code)
    end

    # A metadata value as an Integer; nil when absent or unreadable (a server bug, not a failure).
    def metadata_integer(key) = Wire::Int64.load(metadata[key])

    # 0-based position of the failing item when a whole EnqueueBatch failed on one item.
    def batch_index = metadata_integer("index")

    # `{queue:, pending:, cap:}` for a QUEUE_FULL carrying all three, else nil.
    def queue_full
      return nil unless reason == Reason::QUEUE_FULL

      queue = metadata["queue"]
      pending = metadata_integer("pending")
      cap = metadata_integer("cap")
      return nil if queue.nil? || pending.nil? || cap.nil?

      { queue: queue, pending: pending, cap: cap }
    end

    # The cap on concurrent watches a WATCH_LIMIT credential is holding, else nil.
    def watch_limit = reason == Reason::WATCH_LIMIT ? metadata_integer("cap") : nil

    # The scope a SCOPE_DENIED credential lacked, else nil.
    def scope = reason == Reason::SCOPE_DENIED ? metadata["scope"] : nil
  end
end
