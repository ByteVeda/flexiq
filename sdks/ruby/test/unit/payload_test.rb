# frozen_string_literal: true

require_relative "../test_helper"

class PayloadTest < Minitest::Test
  def test_symbol_keyword_arguments_encode_as_text_keys
    assert_equal "028280a1616bf5", hex(FlexiQ::Payload.encode_call([], { k: true }))
  end

  def test_a_result_is_a_bare_value
    assert(FlexiQ::Payload.decode_result(unhex("02f5")))
    assert_equal 2**53, FlexiQ::Payload.decode_result(unhex("021b0020000000000000"))
  end

  def test_a_result_encodes_as_a_bare_tagged_value
    assert_equal "02f5", hex(FlexiQ::Payload.encode_result(true))
    assert_equal({ "n" => [1, "x"] }, FlexiQ::Payload.decode_result(FlexiQ::Payload.encode_result({ "n" => [1, "x"] })))
  end

  def test_an_unencodable_result_is_a_codec_error
    assert_raises(FlexiQ::CodecError) { FlexiQ::Payload.encode_result(Object.new) }
  end

  def test_foreign_codec_tags_are_named
    error = assert_raises(FlexiQ::CodecError) { FlexiQ::Payload.decode(unhex("0080")) }
    assert_match(/0x00 \(language-native\)/, error.message)

    error = assert_raises(FlexiQ::CodecError) { FlexiQ::Payload.decode(unhex("0190")) }
    assert_match(/0x01 \(MessagePack\)/, error.message)

    error = assert_raises(FlexiQ::CodecError) { FlexiQ::Payload.decode(unhex("07")) }
    assert_match(/0x07 \(reserved\)/, error.message)
  end

  def test_empty_payload_is_refused
    assert_raises(FlexiQ::CodecError) { FlexiQ::Payload.decode("".b) }
  end

  def test_a_call_body_must_be_args_and_kwargs
    assert_raises(FlexiQ::CodecError) { FlexiQ::Payload.decode_call(unhex("02f5")) }
    assert_raises(FlexiQ::CodecError) { FlexiQ::Payload.decode_call(unhex("028180")) }
  end

  def test_args_and_kwargs_types_are_checked
    assert_raises(ArgumentError) { FlexiQ::Payload.encode_call({}, {}) }
    assert_raises(ArgumentError) { FlexiQ::Payload.encode_call([], []) }
  end
end
