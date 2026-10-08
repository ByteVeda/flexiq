# frozen_string_literal: true

require_relative "../test_helper"

class SSETest < Minitest::Test
  Event = FlexiQ::Wire::SSE::Event

  def parse(*chunks)
    parser = FlexiQ::Wire::SSE::Parser.new
    events = []
    chunks.each { |chunk| parser.feed(chunk) { |event| events << event } }
    events
  end

  def test_the_doors_framing_reads_as_one_event_each
    events = parse("data: {\"cursor\":\"c1\"}\nid: c1\n\n" \
                   ": keepalive\n\n" \
                   "event: end\ndata: {}\n\n")

    assert_equal [Event.new(id: "c1", type: "message", data: "{\"cursor\":\"c1\"}"),
                  Event.new(id: nil, type: "end", data: "{}")], events
  end

  def test_multi_line_data_joins_with_newlines
    assert_equal ["a\n\nb"], parse("data: a\ndata\ndata:b\n\n").map(&:data)
  end

  def test_only_one_leading_space_is_stripped
    assert_equal [" x "], parse("data:  x \n\n").map(&:data)
  end

  def test_every_line_ending_works
    expected = [Event.new(id: "1", type: "message", data: "a")] * 3

    assert_equal expected, parse("id: 1\r\ndata: a\r\n\r\nid: 1\rdata: a\r\rid: 1\ndata: a\n\n")
  end

  def test_a_line_or_event_split_across_chunks_is_held_until_complete
    events = parse("da", "ta: {\"a\"", ":1}\r", "\n", "\r", "\ndata: b\n", "\n")

    assert_equal ["{\"a\":1}", "b"], events.map(&:data)
  end

  def test_a_cr_ending_one_chunk_dispatches_without_waiting_for_the_next
    parser = FlexiQ::Wire::SSE::Parser.new
    events = []
    parser.feed("data: a\r\r") { |event| events << event }

    assert_equal ["a"], events.map(&:data)
  end

  def test_comments_retry_unknown_fields_and_empty_events_dispatch_nothing
    assert_empty parse(": keepalive\n\nretry: 10\nbogus: 1\n\nevent: end\n\n")
  end

  def test_an_unfinished_event_never_dispatches
    assert_empty parse("data: half")
    assert_empty parse("data: half\n")
  end

  def test_an_id_holding_nul_is_ignored_and_ids_do_not_carry_over
    events = parse("id: a\0b\ndata: x\n\ndata: y\n\nid: c\ndata: z\n\n")

    assert_equal [nil, nil, "c"], events.map(&:id)
  end

  def test_an_unfinished_line_or_event_past_the_bound_is_refused
    parser = FlexiQ::Wire::SSE::Parser.new(max_bytes: 4)
    parser.feed("data: 123\n") { nil } # "123\n" sits exactly at the bound
    error = assert_raises(FlexiQ::TransportError) { parser.feed("data: 4\n") { nil } }

    assert_match(/event/, error.message)
    error = assert_raises(FlexiQ::TransportError) do
      FlexiQ::Wire::SSE::Parser.new(max_bytes: 4).feed("data: 1") { nil }
    end
    assert_match(/line/, error.message)
  end

  def test_data_is_utf8_and_a_malformed_byte_is_replaced
    events = parse("data: caf\xC3".b, "\xA9 \xFF\n\n".b)

    assert_equal Encoding::UTF_8, events.first.data.encoding
    assert_equal "café �", events.first.data
  end
end
