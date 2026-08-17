package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A workflow could not be driven to completion — e.g. a run was not found, a
 * deferred node has no registered payload, a callable condition was not
 * registered on the worker, or awaiting a run was interrupted.
 */
public class WorkflowException extends FlexiQException {
    public WorkflowException(String message) {
        super(message);
    }

    public WorkflowException(String message, Throwable cause) {
        super(message, cause);
    }
}
