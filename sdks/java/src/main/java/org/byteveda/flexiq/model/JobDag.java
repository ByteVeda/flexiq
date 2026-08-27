package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** A job's dependency graph: full job rows as nodes plus directed edges. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobDag {
    /** Every job in the graph, as full rows. */
    public final List<Job> nodes;

    /** The dependencies between them. */
    public final List<DagEdge> edges;

    /**
     * Decoded from the core's JSON graph view; both lists default to empty.
     *
     * @param nodes every job in the graph, as full rows
     * @param edges the dependencies between them
     */
    @JsonCreator
    public JobDag(@JsonProperty("nodes") List<Job> nodes, @JsonProperty("edges") List<DagEdge> edges) {
        this.nodes = nodes == null ? List.of() : List.copyOf(nodes);
        this.edges = edges == null ? List.of() : List.copyOf(edges);
    }
}
