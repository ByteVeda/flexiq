# frozen_string_literal: true

module FlexiQ
  # What `Client#get_workflow_run` returns: the run and every node it has, in no set order.
  WorkflowRunView = Data.define(:run, :nodes) do
    def self.from_json(json)
      run = json["run"]
      raise TransportError, "the server answered without a workflow run" unless run.is_a?(Hash)

      nodes = json.fetch("nodes", [])
      raise TransportError, "the server answered a run whose nodes are not a list" unless nodes.is_a?(Array)

      new(run: WorkflowRun.from_json(run), nodes: nodes.map { |node| WorkflowNodeState.from_json(node) }.freeze)
    end

    # The node named `name`, or nil.
    def node(name) = nodes.find { |node| node.name == name }
  end
end
