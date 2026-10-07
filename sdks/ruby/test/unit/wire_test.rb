# frozen_string_literal: true

require_relative "../test_helper"

class WireTest < Minitest::Test
  def test_durations_use_the_servers_digit_groups
    assert_equal "30s", FlexiQ::Wire::Duration.dump(30)
    assert_equal "1.500s", FlexiQ::Wire::Duration.dump(1.5)
    assert_equal "0.000250s", FlexiQ::Wire::Duration.dump(Rational(1, 4000))
    assert_equal "0.000000001s", FlexiQ::Wire::Duration.dump(Rational(1, 1_000_000_000))
  end

  def test_negative_or_non_finite_durations_are_refused
    assert_raises(ArgumentError) { FlexiQ::Wire::Duration.dump(-1) }
    assert_raises(ArgumentError) { FlexiQ::Wire::Duration.dump(Float::INFINITY) }
    assert_raises(ArgumentError) { FlexiQ::Wire::Duration.dump("30s") }
  end

  def test_durations_load_exactly
    assert_equal Rational(3, 2), FlexiQ::Wire::Duration.load("1.500s")
    assert_equal(-1, FlexiQ::Wire::Duration.load("-1s"))
    assert_nil FlexiQ::Wire::Duration.load("soon")
  end

  def test_timestamps_round_trip_in_utc
    time = Time.utc(2026, 1, 2, 3, 4, 5, 678_901)
    text = FlexiQ::Wire::Timestamp.dump(time)

    assert_equal "2026-01-02T03:04:05.678901000Z", text
    assert_equal time, FlexiQ::Wire::Timestamp.load(text)
    assert_nil FlexiQ::Wire::Timestamp.load("yesterday")
  end

  def test_bytes_read_either_alphabet_with_or_without_padding
    assert_equal "AQL+/w==", FlexiQ::Wire::Bytes.dump("\x01\x02\xFE\xFF".b)
    assert_equal "\x01\x02\xFE\xFF".b, FlexiQ::Wire::Bytes.load("AQL-_w")
    assert_raises(FlexiQ::CodecError) { FlexiQ::Wire::Bytes.load("!!!") }
  end

  def test_int64_reads_strings_and_rejects_junk
    assert_equal 9_007_199_254_740_993, FlexiQ::Wire::Int64.load("9007199254740993")
    assert_nil FlexiQ::Wire::Int64.load("12abc")
    assert_nil FlexiQ::Wire::Int64.load(nil)
  end

  def test_path_segments_cannot_add_segments_or_verbs
    assert_equal "a%2Fb%3Acancel%20x", FlexiQ::Wire::Path.segment("a/b:cancel x")
    assert_equal "%C3%A9", FlexiQ::Wire::Path.segment("é")
    assert_raises(ArgumentError) { FlexiQ::Wire::Path.segment("") }
  end
end
