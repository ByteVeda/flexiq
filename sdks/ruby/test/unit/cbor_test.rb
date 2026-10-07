# frozen_string_literal: true

require_relative "../test_helper"

class CBORTest < Minitest::Test
  def encode(value) = hex(FlexiQ::CBOR::Encoder.encode(value))

  def decode(text) = FlexiQ::CBOR::Decoder.decode(unhex(text))

  def test_integers_take_the_shortest_head
    { 0 => "00", 23 => "17", 24 => "1818", 255 => "18ff", 256 => "190100",
      65_536 => "1a00010000", 2**32 => "1b0000000100000000", -1 => "20", -25 => "3818" }.each do |value, bytes|
      assert_equal bytes, encode(value), "encoding #{value}"
    end
  end

  def test_integer_boundaries_of_the_64_bit_range
    assert_equal "1bffffffffffffffff", encode((2**64) - 1)
    assert_equal "3bffffffffffffffff", encode(-(2**64))
    assert_raises(FlexiQ::CodecError) { encode(2**64) }
  end

  def test_finite_floats_are_always_64_bit
    assert_equal "fb3ff8000000000000", encode(1.5)
    assert_equal "fb0000000000000000", encode(0.0)
  end

  def test_non_finite_floats_use_preferred_serialization
    assert_equal "f97c00", encode(Float::INFINITY)
    assert_equal "f9fc00", encode(-Float::INFINITY)
    assert_equal "f97e00", encode(Float::NAN)
  end

  def test_binary_strings_are_byte_strings_and_others_are_text
    assert_equal "420102", encode("\x01\x02".b)
    assert_equal "6161", encode("a")
    assert_equal "6161", encode(:a)
  end

  def test_invalid_utf8_text_is_refused
    assert_raises(FlexiQ::CodecError) { encode((+"\xFF").force_encoding(Encoding::UTF_8)) }
  end

  def test_unsupported_types_are_refused_by_name
    error = assert_raises(FlexiQ::CodecError) { encode(Object.new) }
    assert_match(/Object/, error.message)
  end

  def test_map_keys_keep_insertion_order
    assert_equal "a2616201616102", encode({ "b" => 1, "a" => 2 })
  end

  def test_indefinite_containers_and_strings_decode
    assert_equal [1, 2], decode("9f0102ff")
    assert_equal({ "a" => 1 }, decode("bf616101ff"))
    assert_equal "ab", decode("7f61616162ff")
    assert_equal "\x01\x02".b, decode("5f41014102ff")
  end

  def test_non_shortest_integers_decode
    assert_equal 1, decode("1b0000000000000001")
  end

  def test_narrow_floats_decode
    assert_in_delta 1.5, decode("f93e00")
    assert_in_delta 1.5, decode("fa3fc00000")
    assert_in_delta 5.960464477539063e-08, decode("f90001")
    assert_predicate decode("f97e00"), :nan?
    assert_equal(-Float::INFINITY, decode("f9fc00"))
  end

  def test_tags_are_kept_not_dropped
    assert_equal FlexiQ::CBOR::Tagged.new(tag: 1, value: 0), decode("c100")
  end

  def test_malformed_input_is_a_codec_error
    %w[ff 82 0100 8201ff 7f01ff 18 62c3 fe].each do |text|
      assert_raises(FlexiQ::CodecError, "decoding #{text}") { decode(text) }
    end
  end

  def test_nesting_is_capped
    deep = "#{"81" * 600}00"
    assert_raises(FlexiQ::CodecError) { decode(deep) }
  end
end
