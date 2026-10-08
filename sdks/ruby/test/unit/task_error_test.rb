# frozen_string_literal: true

require_relative "../test_helper"

class TaskErrorTest < Minitest::Test
  def test_canonical_json_is_structured
    error = FlexiQ::TaskError.parse('{"errtype":"ValueError","message":"bad value 42","traceback":["f1","f2"]}')

    assert_predicate error, :structured?
    assert_equal ["ValueError", "bad value 42", %w[f1 f2]], [error.errtype, error.message, error.traceback]
    assert_equal "ValueError: bad value 42", error.to_s
  end

  def test_plain_text_is_surfaced_verbatim
    error = FlexiQ::TaskError.parse("job timed out after 30s")

    refute_predicate error, :structured?
    assert_equal "job timed out after 30s", error.message
    assert_equal "job timed out after 30s", error.to_s
  end

  def test_json_without_a_string_message_is_not_structured
    ['{"errtype":"X"}', '{"message":5}', "[1]", '"text"'].each do |raw|
      error = FlexiQ::TaskError.parse(raw)

      refute_predicate error, :structured?, raw
      assert_equal raw, error.message
    end
  end

  def test_encode_writes_the_canonical_key_order
    assert_equal '{"errtype":"ValueError","message":"bad value 42","traceback":["f1","f2"]}',
                 FlexiQ::TaskError.encode("ValueError", "bad value 42", %w[f1 f2])
    assert_equal '{"errtype":"E","message":"","traceback":[]}', FlexiQ::TaskError.encode("E", "")
  end

  def test_encode_round_trips_through_parse
    error = FlexiQ::TaskError.parse(FlexiQ::TaskError.encode("KeyError", "no \"k\"\n", ["a:1"]))

    assert_predicate error, :structured?
    assert_equal ["KeyError", "no \"k\"\n", ["a:1"]], [error.errtype, error.message, error.traceback]
  end

  def test_missing_siblings_take_defaults
    error = FlexiQ::TaskError.parse('{"message":"m","traceback":null}')

    assert_predicate error, :structured?
    assert_equal ["Error", []], [error.errtype, error.traceback]
  end
end
