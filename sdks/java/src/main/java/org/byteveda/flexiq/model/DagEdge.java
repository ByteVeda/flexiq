package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A directed {@code from → to} dependency edge in a job DAG. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class DagEdge {
    public final String from;
    public final String to;

    @JsonCreator
    public DagEdge(@JsonProperty("from") String from, @JsonProperty("to") String to) {
        this.from = from;
        this.to = to;
    }
}
