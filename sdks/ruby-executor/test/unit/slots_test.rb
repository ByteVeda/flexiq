# frozen_string_literal: true

require_relative "../test_helper"

class SlotsTest < Minitest::Test
  def test_acquire_until_full_then_release
    slots = FlexiQ::Executor::Slots.new(2)

    assert slots.acquire
    assert slots.acquire
    refute slots.acquire
    slots.release

    assert_equal [1, 1], [slots.available, slots.in_flight]
  end

  def test_draining_reports_no_capacity_and_takes_no_work
    slots = FlexiQ::Executor::Slots.new(2)
    slots.drain

    assert_equal 0, slots.available
    refute slots.acquire
    slots.resume

    assert slots.acquire
  end

  def test_await_idle_waits_for_release
    slots = FlexiQ::Executor::Slots.new(1)
    slots.acquire

    refute slots.await_idle(0.05)
    Thread.new { slots.release }

    assert slots.await_idle(2)
  end
end
