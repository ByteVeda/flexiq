# frozen_string_literal: true

module FlexiQ
  # One step of a workflow: its name in the graph, the task it runs, the call and its settings.
  #
  # The arguments travel as the `raw` CBOR envelope, the same bytes `Client#enqueue` sends.
  # `timeout` is seconds; `condition` is a FlexiQ::EdgeCondition symbol; `compensate` names the
  # task a saga rollback runs. `gate`, `cache`, `fan_out`, `fan_in` and `sub_workflow` take their
  # FlexiQ construct (or a Hash of its fields) and are refused by the server; see GateConfig.
  WorkflowNode = Data.define(
    :name, :task_name, :args, :kwargs, :queue, :max_retries, :timeout, :priority, :condition,
    :compensate, :gate, :cache, :fan_out, :fan_in, :sub_workflow
  ) do
    def initialize(name:, task_name:, args: [], kwargs: {}, queue: nil, max_retries: nil, timeout: nil,
                   priority: nil, condition: nil, compensate: nil, gate: nil, cache: nil, fan_out: nil,
                   fan_in: nil, sub_workflow: nil)
      raise ArgumentError, "a node name must be a non-empty String" unless name.is_a?(String) && !name.empty?
      unless task_name.is_a?(String) && !task_name.empty?
        raise ArgumentError, "node #{name.inspect} needs a non-empty task_name"
      end

      super(
        name: name, task_name: task_name, args: args, kwargs: kwargs, queue: queue, max_retries: max_retries,
        timeout: timeout, priority: priority, condition: condition, compensate: compensate,
        gate: coerce(gate, GateConfig), cache: coerce(cache, CacheConfig), fan_out: coerce(fan_out, FanOutConfig),
        fan_in: coerce(fan_in, FanInConfig), sub_workflow: coerce(sub_workflow, SubWorkflowSpec)
      )
    end

    # The proto3 JSON `WorkflowNodeConfig` object, unset fields omitted.
    def to_wire
      {
        "name" => name,
        "taskName" => task_name,
        "queue" => queue,
        "raw" => Wire::Bytes.dump(Payload.encode_call(args, kwargs)),
        "maxRetries" => max_retries,
        "timeout" => timeout && Wire::Duration.dump(timeout),
        "priority" => priority,
        "condition" => EdgeCondition.dump(condition),
        "gate" => gate&.to_wire,
        "cache" => cache&.to_wire,
        "fanOut" => fan_out&.to_wire,
        "fanIn" => fan_in&.to_wire,
        "subWorkflow" => sub_workflow&.to_wire,
        "compensate" => compensate
      }.compact
    end

    private

    def coerce(value, type) = value.is_a?(Hash) ? type.new(**value) : value
  end
end
