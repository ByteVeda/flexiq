package org.byteveda.flexiq.workflows;

/**
 * A predicate deciding whether a workflow step runs, given the run's state when
 * its predecessors have settled. Registered with the running worker via
 * {@code trackWorkflows(workflow)} — it is code, so it is not persisted; a
 * workflow using a callable condition must be tracked on the worker that runs it.
 */
@FunctionalInterface
public interface Condition {
    boolean test(WorkflowContext context);
}
