# frozen_string_literal: true

require_relative "../test_helper"

class EnqueueRequestTest < Minitest::Test
  def test_minimal_request_is_task_and_raw_body
    wire = FlexiQ::EnqueueRequest.new(task_name: "t").to_wire

    assert_equal({ "taskName" => "t", "raw" => "AoKAoA==" }, wire)
  end

  def test_options_render_as_proto3_json
    options = {
      queue: "emails", priority: 3, max_retries: 2, scheduled_at: Time.utc(2026, 1, 1), timeout: 1.5,
      unique_key: "k", depends_on: %w[a b], result_ttl: 60,
      debounce: { key: "burst", window: 2, max_pending: 10, replace_payload: true }
    }
    wire = FlexiQ::EnqueueRequest.new(task_name: "t", options: options).to_wire["options"]

    assert_equal({
                   "queue" => "emails", "priority" => 3, "maxRetries" => 2,
                   "scheduledAt" => "2026-01-01T00:00:00.000000000Z", "timeout" => "1.500s",
                   "uniqueKey" => "k", "dependsOn" => %w[a b], "resultTtl" => "60s",
                   "debounce" => { "key" => "burst", "window" => "2s", "replacePayload" => true, "maxPending" => "10" }
                 }, wire)
  end

  def test_unknown_options_and_bad_task_names_are_refused
    assert_raises(ArgumentError) { FlexiQ::EnqueueOptions.new(qeueu: "x") }
    assert_raises(ArgumentError) { FlexiQ::EnqueueRequest.new(task_name: "") }
    assert_raises(ArgumentError) { FlexiQ::EnqueueOptions.new(scheduled_at: "tomorrow").to_wire }
  end
end
