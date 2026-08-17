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
    public final String id;
    public final String definitionId;
    public final WorkflowState state;
    public final String params;
    public final String error;
    public final Long startedAt;
    public final Long completedAt;
    public final long createdAt;
    public final String parentRunId;
    public final String parentNodeName;

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
