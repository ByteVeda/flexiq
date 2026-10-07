# frozen_string_literal: true

require_relative "../test_helper"
require "json"

# contracts/wire-vectors.json is the conformance bar: decode every case, produce the exact bytes
# of every `encode` case, round-trip the `round_trip_only` ones, and never write a narrow float.
class WireVectorsTest < Minitest::Test
  VECTORS = JSON.parse(File.read(File.join(REPO_ROOT, "contracts", "wire-vectors.json")))

  def test_schema_version_is_one_this_suite_understands
    assert_equal 1, VECTORS["$schema_version"]
  end

  VECTORS["encode"].each do |vector|
    define_method("test_encode_#{vector["name"].tr("-", "_")}") do
      assert_equal vector["hex"], hex(FlexiQ::Payload.encode_call(vector["args"], vector["kwargs"]))
    end

    define_method("test_decode_#{vector["name"].tr("-", "_")}") do
      assert_equal [vector["args"], vector["kwargs"]], FlexiQ::Payload.decode_call(unhex(vector["hex"]))
    end
  end

  VECTORS["decode_only"].each do |vector|
    define_method("test_decode_only_#{vector["name"].tr("-", "_")}") do
      args, kwargs = FlexiQ::Payload.decode_call(unhex(vector["hex"]))
      if vector["round_trip_only"]
        assert_equal vector["hex"], hex(FlexiQ::Payload.encode_call(args, kwargs))
      else
        assert_equal [vector["args"], vector["kwargs"]], [args, kwargs]
        refute_equal vector["hex"], hex(FlexiQ::Payload.encode_call(args, kwargs)), "wrote a forbidden float width"
      end
    end
  end
end
