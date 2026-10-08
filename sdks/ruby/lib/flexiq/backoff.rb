# frozen_string_literal: true

module FlexiQ
  # Retry delay: 0.25 s doubling to 5 s; `reset` after a success.
  class Backoff
    START = 0.25
    CAP = 5.0

    def initialize = reset

    def reset
      @delay = START
    end

    # Sleeps the current delay, cut short by `deadline` (Deadline) when given.
    def pause(deadline = nil)
      sleep(deadline ? deadline.cap(@delay) : @delay)
      @delay = [@delay * 2, CAP].min
    end
  end
end
