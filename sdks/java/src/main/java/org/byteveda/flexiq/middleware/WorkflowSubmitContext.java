package org.byteveda.flexiq.middleware;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A workflow being submitted, passed to {@link Middleware#onWorkflowSubmit}.
 * {@link #traceContext()} is the W3C carrier ({@code traceparent},
 * {@code tracestate}) every node job of the run will carry; write into it to
 * supply one when the caller passed none.
 */
public final class WorkflowSubmitContext {
    /** The workflow's name. */
    public final String workflowName;

    private final Map<String, String> traceContext;

    /**
     * The submit as the caller framed it, before any hook has run.
     *
     * @param workflowName the workflow's name
     * @param traceContext the carrier the caller passed; empty for none
     */
    public WorkflowSubmitContext(String workflowName, Map<String, String> traceContext) {
        this.workflowName = workflowName;
        this.traceContext = new LinkedHashMap<>(traceContext);
    }

    /**
     * The trace carrier, as it stands after the hooks that ran already.
     *
     * @return the live map — write into it to supply a carrier
     */
    public Map<String, String> traceContext() {
        return traceContext;
    }
}
