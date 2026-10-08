# frozen_string_literal: true

module FlexiQ
  class Watch
    # Queue watch. Resumes from the last cursor of any item; an expired one yields a WatchGap
    # and restarts from now.
    class ByQueue < Watch
      def initialize(transport, queue, resume_cursor)
        super(transport)
        @queue = queue.to_s
        @cursor = resume_cursor.nil? || resume_cursor.empty? ? nil : resume_cursor
      end

      private

      def query = { "queue" => @queue, "resumeCursor" => @cursor }

      def show?(item)
        @cursor = item.cursor if item.respond_to?(:cursor) && item.cursor
        true
      end

      def recover(error, block)
        return super unless error.reason == Reason::WATCH_CURSOR_EXPIRED && @cursor

        lost = @cursor
        @cursor = nil
        hand_over(WatchGap.new(lost_cursor: lost), block)
        :now
      end
    end
  end
end
