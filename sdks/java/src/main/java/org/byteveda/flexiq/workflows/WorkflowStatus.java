package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** A snapshot of a workflow run's state and its nodes. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WorkflowStatus {
    /** The run this describes. */
    public final String runId;

    /** Where the run stands right now. */
    public final WorkflowState state;

    /** When the first node was dispatched, in Unix milliseconds, or {@code null}. */
    public final Long startedAt;

    /** When the run reached a terminal state, in Unix milliseconds, or {@code null}. */
    public final Long completedAt;

    /** Why the run failed, or {@code null}. */
    public final String error;

    /** Every node's snapshot; empty rather than null when the core reported none. */
    public final List<NodeSnapshot> nodes;

    /**
     * Decoded from the core's JSON run view.
     *
     * @param runId the run this describes
     * @param state where the run stands right now
     * @param startedAt when the first node was dispatched, in Unix milliseconds, or {@code null}
     * @param completedAt when the run reached a terminal state, in Unix milliseconds, or {@code null}
     * @param error why the run failed, or {@code null}
     * @param nodes every node's snapshot; empty rather than null when the core reported none
     */
    @JsonCreator
    public WorkflowStatus(
            @JsonProperty("runId") String runId,
            @JsonProperty("state") WorkflowState state,
            @JsonProperty("startedAt") Long startedAt,
            @JsonProperty("completedAt") Long completedAt,
            @JsonProperty("error") String error,
            @JsonProperty("nodes") List<NodeSnapshot> nodes) {
        this.runId = runId;
        this.state = state;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.error = error;
        this.nodes = nodes == null ? Collections.emptyList() : Collections.unmodifiableList(nodes);
    }

    /**
     * Whether the run has reached a final state.
     *
     * @return whether {@link #state} is terminal
     */
    public boolean isTerminal() {
        return state.isTerminal();
    }

    /**
     * The named node, if present.
     *
     * @param nodeName the node's name within the workflow
     * @return its snapshot, or empty when the run has no such node
     */
    public Optional<NodeSnapshot> node(String nodeName) {
        return nodes.stream().filter(n -> n.nodeName.equals(nodeName)).findFirst();
    }

    /**
     * The name of the first failed node, if any (helps explain a {@code FAILED} run).
     *
     * @return the node's name, or empty when nothing failed
     */
    public Optional<String> failedStep() {
        return nodes.stream()
                .filter(n -> n.status == NodeStatus.FAILED)
                .map(n -> n.nodeName)
                .findFirst();
    }
}
