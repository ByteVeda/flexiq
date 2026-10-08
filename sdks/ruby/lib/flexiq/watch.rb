# frozen_string_literal: true

require "json"

module FlexiQ
  # Internal: one watch kept alive across streams. Behind Client#watch_jobs / #watch_queue.
  #
  # Reopens a stream that drops for a reason that clears. Before the first item, every failure
  # raises, so a bad URL or a token at its cap fails loudly instead of spinning.
  class Watch
    PATH = "/v1/jobs:watch"

    # Contract: reopen on these.
    REOPEN = [Reason::WATCH_OVERFLOW, Reason::SHUTTING_DOWN].freeze

    # `deadline` (Deadline): raise WaitTimeoutError once it passes.
    def initialize(transport, deadline: nil)
      @transport = transport
      @deadline = deadline
      @backoff = Backoff.new
      @opened = false
      @caller_error = nil
    end

    # Yields items until finished; breaking out closes the stream.
    def run(&block)
      until finished?
        check_deadline
        pace = attempt(block)
        @backoff.pause(@deadline) if pace == :later && !finished?
      end
    end

    private

    # The query this watch opens its next stream with.
    def query = raise NotImplementedError

    def finished? = false

    # Records an item; true when the caller should see it.
    def show?(_item) = true

    # How soon to reopen after `error`: :now, :later, or nil to raise it.
    def recover(error, _block)
      return :later if REOPEN.include?(error.reason)

      @opened && error.retryable? ? :later : nil
    end

    # One stream, open to close. Returns how soon to open the next.
    def attempt(block)
      read(block)
      :later # closed cleanly before the watch finished: the server ended it early
    rescue RPCError => e
      raise @caller_error if @caller_error

      recover(e, block) || raise
    rescue TransportError
      # The transport wraps an IOError or SystemCallError the block raised; hand back the original.
      raise @caller_error if @caller_error

      check_deadline # a read cut short by the deadline is a timeout, not a drop
      raise unless @opened

      :later
    end

    def check_deadline
      raise WaitTimeoutError, "gave up after #{@deadline.seconds}s" if @deadline&.expired?
    end

    def read(block)
      parser = Wire::SSE::Parser.new
      ended = false
      @transport.stream(PATH, query, deadline: @deadline) do |chunk|
        check_deadline # keepalives alone would keep a read alive past it
        parser.feed(chunk) do |event|
          case event.type
          when "message" then deliver(item(event.data), block)
          when "error" then raise failure(event.data)
          when "end" then ended = true # the server closes next
          end
        end
      end
      raise TransportError, "the watch closed without an end or error event" unless ended
    end

    def deliver(item, block)
      @opened = true
      @backoff.reset
      hand_over(item, block) if show?(item)
    end

    # Tags the block's own errors so they are never mistaken for the stream failing.
    def hand_over(item, block)
      block.call(item)
    rescue StandardError => e
      @caller_error = e
      raise
    end

    # One WatchJobsResponse. An unknown arm reads as a checkpoint, keeping its cursor.
    def item(data)
      json = parse_json(data)
      raise TransportError, "a watch item is not a JSON object: #{data[0, 200]}" unless json.is_a?(Hash)

      cursor = json["cursor"].is_a?(String) && !json["cursor"].empty? ? json["cursor"] : nil
      if json["transition"].is_a?(Hash)
        JobTransition.from_json(json["transition"], cursor: cursor)
      elsif json["notFoundJobId"].is_a?(String)
        JobNotFound.new(job_id: json["notFoundJobId"])
      else
        WatchCheckpoint.new(cursor: cursor)
      end
    end

    # An `error` event carries the facade's usual error body.
    def failure(data)
      body = parse_json(data)
      return RPCError.from_status(body["error"]) if body.is_a?(Hash) && body["error"].is_a?(Hash)

      TransportError.new("the watch failed with an unreadable error event: #{data[0, 200]}")
    end

    def parse_json(data)
      JSON.parse(data)
    rescue JSON::ParserError
      nil
    end
  end
end
