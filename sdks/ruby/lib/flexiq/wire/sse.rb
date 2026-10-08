# frozen_string_literal: true

module FlexiQ
  module Wire
    # WHATWG `text/event-stream`, the framing of a watch.
    module SSE
      # `id`: this event's own id field, or nil. `type`: "message" unless `event` set one.
      Event = Data.define(:id, :type, :data)

      # Incremental parser. Holds partial lines across chunks; skips comments and `retry`;
      # an event never ended by a blank line is dropped.
      class Parser
        LINE_BREAK = /[\r\n]/
        CR = 13
        LF = 10
        private_constant :LINE_BREAK, :CR, :LF

        # Bound on an unfinished line and on one event's data. A watch item is a few hundred
        # bytes; a peer that never ends a line or event must not grow memory without bound.
        MAX_BYTES = 1 << 20

        def initialize(max_bytes: MAX_BYTES)
          @max_bytes = max_bytes
          @buffer = "".b
          @after_cr = false
          reset_event
        end

        # Yields each event the chunk completes.
        def feed(chunk, &)
          @buffer << chunk.b
          drop_lf_after_cr
          while (index = @buffer.index(LINE_BREAK))
            line = @buffer.byteslice(0, index)
            @after_cr = @buffer.getbyte(index) == CR
            @buffer = @buffer.byteslice((index + 1)..)
            drop_lf_after_cr
            process(line, &)
          end
          limit!(@buffer, "line")
        end

        private

        # CR ends a line at once; skip the LF of a CRLF split across chunks.
        def drop_lf_after_cr
          return if !@after_cr || @buffer.empty?

          @buffer = @buffer.byteslice(1..) if @buffer.getbyte(0) == LF
          @after_cr = false
        end

        def process(line, &)
          return dispatch(&) if line.empty?
          return if line.start_with?(":")

          field, value = line.split(":", 2)
          value = value.nil? ? "".b : value.delete_prefix(" ")
          case field
          when "data" then limit!(@data << value << "\n", "event")
          when "event" then @type = value
          when "id" then @id = value unless value.include?("\0")
          end
        end

        def limit!(bytes, what)
          return if bytes.bytesize <= @max_bytes

          raise TransportError, "an event-stream #{what} passed #{@max_bytes} bytes unfinished"
        end

        def dispatch
          unless @data.empty?
            type = @type.nil? || @type.empty? ? "message" : text(@type)
            yield Event.new(id: @id && text(@id), type: type, data: text(@data.chomp))
          end
          reset_event
        end

        def reset_event
          @id = nil
          @type = nil
          @data = "".b
        end

        # UTF-8; malformed bytes become U+FFFD.
        def text(bytes) = bytes.dup.force_encoding(Encoding::UTF_8).scrub
      end
    end
  end
end
