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
    public final String runId;
    public final WorkflowState state;
    public final Long startedAt;
    public final Long completedAt;
    public final String error;
    public final List<NodeSnapshot> nodes;

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

    /** Whether the run has reached a final state. */
    public boolean isTerminal() {
        return state.isTerminal();
    }

    /** The named node, if present. */
    public Optional<NodeSnapshot> node(String nodeName) {
        return nodes.stream().filter(n -> n.nodeName.equals(nodeName)).findFirst();
    }

    /** The name of the first failed node, if any (helps explain a {@code FAILED} run). */
    public Optional<String> failedStep() {
        return nodes.stream()
                .filter(n -> n.status == NodeStatus.FAILED)
                .map(n -> n.nodeName)
                .findFirst();
    }
}
