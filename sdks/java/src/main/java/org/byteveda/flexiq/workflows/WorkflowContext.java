package org.byteveda.flexiq.workflows;

import java.util.Map;
import java.util.Optional;

/**
 * The run's state passed to a callable {@link Condition} when deciding whether a
 * step should run: the results and statuses of already-settled nodes.
 *
 * @param runId the workflow run id
 * @param results deserialized results of completed nodes, by node name
 * @param statuses every settled node's status, by node name
 * @param successCount how many nodes completed
 * @param failureCount how many nodes failed
 */
public record WorkflowContext(
        String runId,
        Map<String, Object> results,
        Map<String, NodeStatus> statuses,
        int successCount,
        int failureCount) {

    /** The result of {@code nodeName}, if it completed. */
    public Optional<Object> result(String nodeName) {
        return Optional.ofNullable(results.get(nodeName));
    }

    /** The status of {@code nodeName}, if it has settled. */
    public Optional<NodeStatus> status(String nodeName) {
        return Optional.ofNullable(statuses.get(nodeName));
    }
}
