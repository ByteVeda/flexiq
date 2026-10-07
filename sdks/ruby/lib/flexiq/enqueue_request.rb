# frozen_string_literal: true

module FlexiQ
  # One job to submit: a task name, its call arguments and its options.
  #
  # The arguments travel as the `raw` CBOR envelope, so the bytes — and any `auto:` key derived
  # from them — match every other FlexiQ runtime's. Prefer one Hash argument (`args: [{...}]`);
  # it maps onto every runtime's handler-binding model.
  EnqueueRequest = Data.define(:task_name, :args, :kwargs, :options) do
    def initialize(task_name:, args: [], kwargs: {}, options: EnqueueOptions.new)
      raise ArgumentError, "task_name must be a non-empty String" unless task_name.is_a?(String) && !task_name.empty?

      options = EnqueueOptions.new(**options) if options.is_a?(Hash)
      super
    end

    # The proto3 JSON `EnqueueRequest` object.
    def to_wire
      wire = { "taskName" => task_name, "raw" => Wire::Bytes.dump(Payload.encode_call(args, kwargs)) }
      opts = options.to_wire
      wire["options"] = opts unless opts.empty?
      wire
    end
  end
end
