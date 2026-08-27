package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A workflow could not be driven to completion — e.g. a run was not found, a
 * deferred node has no registered payload, a callable condition was not
 * registered on the worker, or awaiting a run was interrupted.
 */
public class WorkflowException extends FlexiQException {
    /**
     * A workflow operation the SDK refused.
     *
     * @param message which run or node could not be driven, and what was missing
     */
    public WorkflowException(String message) {
        super(message);
    }

    /**
     * A workflow operation that failed because something underneath did.
     *
     * @param message which run or node could not be driven, and what was missing
     * @param cause the interruption or native failure underneath
     */
    public WorkflowException(String message, Throwable cause) {
        super(message, cause);
    }
}
