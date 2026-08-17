package org.byteveda.flexiq.events;

import org.jspecify.annotations.Nullable;

/**
 * One workflow node's compensation progress — {@link EventName#WORKFLOW_NODE_COMPENSATING},
 * {@link EventName#WORKFLOW_NODE_COMPENSATED}, or
 * {@link EventName#WORKFLOW_NODE_COMPENSATION_FAILED}.
 *
 * @param name which compensation transition occurred
 * @param runId the workflow run's id
 * @param nodeName the node being rolled back
 * @param error the compensation failure; null unless the rollback failed
 */
public record NodeCompensationEvent(EventName name, String runId, String nodeName, @Nullable String error)
        implements FlexiQEvent {

    /** Validates {@code name} is a node-compensation constant. */
    public NodeCompensationEvent {
        if (name != EventName.WORKFLOW_NODE_COMPENSATING
                && name != EventName.WORKFLOW_NODE_COMPENSATED
                && name != EventName.WORKFLOW_NODE_COMPENSATION_FAILED) {
            throw new IllegalArgumentException(name + " is not a node-compensation event");
        }
    }
}
