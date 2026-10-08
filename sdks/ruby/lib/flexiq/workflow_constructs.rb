# frozen_string_literal: true

module FlexiQ
  # The dynamic workflow constructs. `Client#submit_workflow` refuses any node setting one, with
  # reason WORKFLOW_CONSTRUCT_UNSUPPORTED: only a live SDK process can advance them. They are
  # here because the wire carries them and the refusal names the field it refused.
  #
  # Durations are seconds; an unset field is omitted, and an empty construct still counts as set.

  # Pause the node for an external decision. `on_timeout` is `:approve` or `:reject`.
  GateConfig = Data.define(:timeout, :on_timeout, :message) do
    def initialize(timeout: nil, on_timeout: nil, message: nil) = super

    def to_wire
      {
        "timeout" => timeout && Wire::Duration.dump(timeout),
        "onTimeout" => on_timeout && on_timeout_wire,
        "message" => message
      }.compact
    end

    private

    def on_timeout_wire
      case on_timeout
      when :approve then "ON_TIMEOUT_APPROVE"
      when :reject then "ON_TIMEOUT_REJECT"
      else raise ArgumentError, "a gate's on_timeout must be :approve or :reject, got #{on_timeout.inspect}"
      end
    end
  end

  # Memoize the node's result; nil `ttl` caches indefinitely.
  CacheConfig = Data.define(:ttl) do
    def initialize(ttl: nil) = super

    def to_wire = { "ttl" => ttl && Wire::Duration.dump(ttl) }.compact
  end

  # Expand the node into one job per item of `items_from`'s result.
  FanOutConfig = Data.define(:items_from) do
    def initialize(items_from: nil) = super

    def to_wire = { "itemsFrom" => items_from }.compact
  end

  # Collect the fan-out node `from` back into one node.
  FanInConfig = Data.define(:from) do
    def to_wire = { "from" => from }
  end

  # Run a child workflow as this node.
  SubWorkflowSpec = Data.define(:name, :version, :graph, :deferred_node_names) do
    def initialize(name:, graph:, version: nil, deferred_node_names: [])
      graph = WorkflowGraph.new(**graph) if graph.is_a?(Hash)
      super
    end

    def to_wire
      {
        "name" => name, "version" => version, "graph" => graph.to_wire,
        "deferredNodeNames" => deferred_node_names.empty? ? nil : deferred_node_names
      }.compact
    end
  end
end
