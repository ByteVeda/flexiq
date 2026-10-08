# frozen_string_literal: true

module FlexiQ
  class Watch
    # Id watch. Resumes by reopening on unfinished ids; each gets a fresh snapshot.
    class ByIds < Watch
      def initialize(transport, job_ids)
        super(transport)
        @pending = Array(job_ids).map(&:to_s).uniq
        raise ArgumentError, "watch_jobs needs at least one job id" if @pending.empty?
      end

      private

      def query = { "jobIds" => @pending }

      def finished? = @pending.empty?

      # No cursor on an id watch, so an unknown arm is dropped.
      def show?(item)
        case item
        when JobTransition
          @pending.delete(item.job_id) if item.terminal?
          true
        when JobNotFound
          @pending.delete(item.job_id)
          true
        else false
        end
      end
    end
  end
end
