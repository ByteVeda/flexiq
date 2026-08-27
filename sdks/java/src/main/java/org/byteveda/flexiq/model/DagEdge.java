package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A directed {@code from → to} dependency edge in a job DAG. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class DagEdge {
    /** The job that must finish first. */
    public final String from;

    /** The job that waits on it. */
    public final String to;

    /**
     * Decoded from the core's JSON graph view.
     *
     * @param from the job that must finish first
     * @param to the job that waits on it
     */
    @JsonCreator
    public DagEdge(@JsonProperty("from") String from, @JsonProperty("to") String to) {
        this.from = from;
        this.to = to;
    }
}
