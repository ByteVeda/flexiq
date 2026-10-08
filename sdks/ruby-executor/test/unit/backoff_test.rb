# frozen_string_literal: true

require_relative "../test_helper"

class BackoffTest < Minitest::Test
  def test_delays_stay_within_a_doubling_ceiling
    backoff = FlexiQ::Executor::Backoff.new(0.25, 1, random: Random.new(1))
    delays = Array.new(5) { backoff.next_delay }

    assert_in_delta 0.25, delays[0]
    delays.each { |delay| assert_includes 0.25..1, delay }
    backoff.reset

    assert_in_delta 0.25, backoff.next_delay
  end
end
