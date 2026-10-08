# frozen_string_literal: true

module FlexiQ
  # `JobTransitionKind` as symbols; an unknown kind stays raw. Finished-ness is
  # JobTransition#terminal?, never the kind.
  module TransitionKind
    BY_WIRE_NAME = {
      "JOB_TRANSITION_KIND_SNAPSHOT" => :snapshot,
      "JOB_TRANSITION_KIND_ENQUEUED" => :enqueued,
      "JOB_TRANSITION_KIND_STARTED" => :started,
      "JOB_TRANSITION_KIND_COMPLETED" => :completed,
      "JOB_TRANSITION_KIND_FAILED" => :failed,
      "JOB_TRANSITION_KIND_RETRYING" => :retrying,
      "JOB_TRANSITION_KIND_DEAD" => :dead,
      "JOB_TRANSITION_KIND_CANCELLED" => :cancelled,
      "JOB_TRANSITION_KIND_SLEEPING" => :sleeping
    }.freeze

    KNOWN = BY_WIRE_NAME.values.freeze

    module_function

    def load(value) = BY_WIRE_NAME.fetch(value, value)

    def known?(kind) = KNOWN.include?(kind)
  end
end
