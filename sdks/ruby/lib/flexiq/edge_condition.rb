# frozen_string_literal: true

module FlexiQ
  # `EdgeCondition` as symbols: how a node's inbound edges gate it on its predecessors' outcome.
  # nil (unset) and `:always` both mean no filter.
  module EdgeCondition
    WIRE_NAME = {
      on_success: "EDGE_CONDITION_ON_SUCCESS",
      on_failure: "EDGE_CONDITION_ON_FAILURE",
      always: "EDGE_CONDITION_ALWAYS"
    }.freeze

    module_function

    # The wire name; nil stays nil so the field is omitted. A String passes as-is.
    def dump(condition)
      case condition
      when nil, String then condition
      when Symbol then WIRE_NAME.fetch(condition) { raise ArgumentError, "unknown edge condition #{condition.inspect}" }
      else raise ArgumentError, "an edge condition must be a Symbol or a String, got #{condition.class}"
      end
    end
  end
end
