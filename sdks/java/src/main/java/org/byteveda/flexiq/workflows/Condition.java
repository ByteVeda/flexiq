package org.byteveda.flexiq.workflows;

/**
 * A predicate deciding whether a workflow step runs, given the run's state when
 * its predecessors have settled. Registered with the running worker via
 * {@code trackWorkflows(workflow)} — it is code, so it is not persisted; a
 * workflow using a callable condition must be tracked on the worker that runs it.
 */
@FunctionalInterface
public interface Condition {
    /**
     * Decide whether the step runs.
     *
     * @param context the run's node results and statuses, as they stand now
     * @return {@code true} to run the step, {@code false} to skip it
     */
    boolean test(WorkflowContext context);
}
