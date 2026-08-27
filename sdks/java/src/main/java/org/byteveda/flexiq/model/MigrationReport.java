package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * What one explicit migrate did. Empty version lists mean the database was
 * already current, which is the common case and not an error.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MigrationReport {
    /** Core schema versions applied by this run, in the order applied. */
    public final List<String> applied;
    /** Workflow schema versions applied by this run. */
    public final List<String> workflowApplied;
    /** Terminal jobs the one-time backlog sweep moved into the archive. */
    public final long archivedJobs;
    /** The backend stores no schema, so there was nothing to migrate. */
    public final boolean schemaless;

    /**
     * Decoded from the core's JSON migrate report.
     *
     * @param applied core schema versions applied by this run, in the order applied
     * @param workflowApplied workflow schema versions applied by this run
     * @param archivedJobs terminal jobs the one-time backlog sweep moved into the archive
     * @param schemaless the backend stores no schema, so there was nothing to migrate
     */
    @JsonCreator
    public MigrationReport(
            @JsonProperty("applied") List<String> applied,
            @JsonProperty("workflowApplied") List<String> workflowApplied,
            @JsonProperty("archivedJobs") long archivedJobs,
            @JsonProperty("schemaless") boolean schemaless) {
        this.applied = applied == null ? List.of() : List.copyOf(applied);
        this.workflowApplied = workflowApplied == null ? List.of() : List.copyOf(workflowApplied);
        this.archivedJobs = archivedJobs;
        this.schemaless = schemaless;
    }

    /**
     * Whether this run changed anything.
     *
     * @return {@code true} when nothing was applied and nothing was archived
     */
    public boolean isEmpty() {
        return applied.isEmpty() && workflowApplied.isEmpty() && archivedJobs == 0;
    }
}
