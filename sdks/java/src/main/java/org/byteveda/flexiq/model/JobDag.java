package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** A job's dependency graph: full job rows as nodes plus directed edges. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobDag {
    public final List<Job> nodes;
    public final List<DagEdge> edges;

    @JsonCreator
    public JobDag(@JsonProperty("nodes") List<Job> nodes, @JsonProperty("edges") List<DagEdge> edges) {
        this.nodes = nodes == null ? List.of() : List.copyOf(nodes);
        this.edges = edges == null ? List.of() : List.copyOf(edges);
    }
}
