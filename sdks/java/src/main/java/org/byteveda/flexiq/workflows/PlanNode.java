package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

/** One node of a run's plan: predecessors plus the metadata needed to enqueue deferred work. */
@JsonIgnoreProperties(ignoreUnknown = true)
final class PlanNode {
    final String name;
    final List<String> predecessors;
    final String taskName;
    final String queue;
    final Integer maxRetries;
    final Long timeoutMs;
    final Integer priority;
    final String fanOut;
    final String fanIn;
    final String gate;
    final String condition;
    final String subWorkflow;
    final String compensate;
    final String cache;

    @JsonCreator
    PlanNode(
            @JsonProperty("name") String name,
            @JsonProperty("predecessors") List<String> predecessors,
            @JsonProperty("taskName") String taskName,
            @JsonProperty("queue") String queue,
            @JsonProperty("maxRetries") Integer maxRetries,
            @JsonProperty("timeoutMs") Long timeoutMs,
            @JsonProperty("priority") Integer priority,
            @JsonProperty("fanOut") String fanOut,
            @JsonProperty("fanIn") String fanIn,
            @JsonProperty("gate") String gate,
            @JsonProperty("condition") String condition,
            @JsonProperty("subWorkflow") String subWorkflow,
            @JsonProperty("compensate") String compensate,
            @JsonProperty("cache") String cache) {
        this.name = name;
        this.predecessors = predecessors == null ? Collections.emptyList() : predecessors;
        this.taskName = taskName;
        this.queue = queue;
        this.maxRetries = maxRetries;
        this.timeoutMs = timeoutMs;
        this.priority = priority;
        this.fanOut = fanOut;
        this.fanIn = fanIn;
        this.gate = gate;
        this.condition = condition;
        this.subWorkflow = subWorkflow;
        this.compensate = compensate;
        this.cache = cache;
    }

    /** Whether this node is a fan-out/fan-in node (its job is created by the fan-out machinery). */
    boolean isFanNode() {
        return fanOut != null || fanIn != null;
    }

    int retries() {
        return maxRetries == null ? 3 : maxRetries;
    }

    long timeout() {
        return timeoutMs == null ? 300_000L : timeoutMs;
    }

    int priorityOrDefault() {
        return priority == null ? 0 : priority;
    }

    String queueOrDefault() {
        return queue == null ? "default" : queue;
    }
}
