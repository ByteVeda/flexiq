package org.byteveda.flexiq.events;

import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * A workflow run's lifecycle transition — submitted, finalized (completed /
 * completed with failures / failed / cancelled), or a saga's run-level
 * compensating / compensated / compensation-failed. Per-node compensation is
 * {@link NodeCompensationEvent}; a parked gate is {@link GateEvent}.
 *
 * @param name which transition occurred
 * @param runId the workflow run's id
 * @param workflowName the workflow definition's name; null when not resolvable at the emit site
 * @param error the failure message; null unless the transition carries one
 */
public record WorkflowEvent(EventName name, String runId, @Nullable String workflowName, @Nullable String error)
        implements FlexiQEvent {

    private static final Set<EventName> RUN_EVENTS = Set.of(
            EventName.WORKFLOW_SUBMITTED,
            EventName.WORKFLOW_COMPLETED,
            EventName.WORKFLOW_COMPLETED_WITH_FAILURES,
            EventName.WORKFLOW_FAILED,
            EventName.WORKFLOW_CANCELLED,
            EventName.WORKFLOW_COMPENSATING,
            EventName.WORKFLOW_COMPENSATED,
            EventName.WORKFLOW_COMPENSATION_FAILED);

    /** Validates {@code name} is a run-level workflow constant. */
    public WorkflowEvent {
        if (!RUN_EVENTS.contains(name)) {
            throw new IllegalArgumentException(name + " is not a run-level workflow event");
        }
    }
}
