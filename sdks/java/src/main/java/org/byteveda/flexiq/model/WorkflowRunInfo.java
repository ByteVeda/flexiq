package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.byteveda.flexiq.workflows.WorkflowState;

/**
 * A workflow run summary (no node detail). {@code createdAt} is Unix
 * milliseconds; start/complete are nullable. Named {@code WorkflowRunInfo} to
 * avoid clashing with the live {@code workflows.WorkflowRun} handle.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WorkflowRunInfo {
    /** The run's id. */
    public final String id;

    /** The workflow definition this run was submitted from. */
    public final String definitionId;

    /** Where the run stands right now. */
    public final WorkflowState state;

    /** The run's input parameters as JSON, or {@code null} when it took none. */
    public final String params;

    /** Why the run failed, or {@code null}. */
    public final String error;

    /** When the first node was dispatched, in Unix milliseconds, or {@code null}. */
    public final Long startedAt;

    /** When the run reached a terminal state, in Unix milliseconds, or {@code null}. */
    public final Long completedAt;

    /** When the run was submitted, in Unix milliseconds. */
    public final long createdAt;

    /** The run that spawned this one as a sub-workflow, or {@code null} at the top level. */
    public final String parentRunId;

    /** The parent's node that spawned it, or {@code null} at the top level. */
    public final String parentNodeName;

    /**
     * Decoded from the core's JSON run view.
     *
     * @param id the run's id
     * @param definitionId the workflow definition this run was submitted from
     * @param state where the run stands right now
     * @param params the run's input parameters as JSON, or {@code null} when it took none
     * @param error why the run failed, or {@code null}
     * @param startedAt when the first node was dispatched, in Unix milliseconds, or {@code null}
     * @param completedAt when the run reached a terminal state, in Unix milliseconds, or {@code null}
     * @param createdAt when the run was submitted, in Unix milliseconds
     * @param parentRunId the run that spawned this one as a sub-workflow, or {@code null} at the top level
     * @param parentNodeName the parent's node that spawned it, or {@code null} at the top level
     */
    @JsonCreator
    public WorkflowRunInfo(
            @JsonProperty("id") String id,
            @JsonProperty("definitionId") String definitionId,
            @JsonProperty("state") WorkflowState state,
            @JsonProperty("params") String params,
            @JsonProperty("error") String error,
            @JsonProperty("startedAt") Long startedAt,
            @JsonProperty("completedAt") Long completedAt,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("parentRunId") String parentRunId,
            @JsonProperty("parentNodeName") String parentNodeName) {
        this.id = id;
        this.definitionId = definitionId;
        this.state = state;
        this.params = params;
        this.error = error;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.createdAt = createdAt;
        this.parentRunId = parentRunId;
        this.parentNodeName = parentNodeName;
    }
}
