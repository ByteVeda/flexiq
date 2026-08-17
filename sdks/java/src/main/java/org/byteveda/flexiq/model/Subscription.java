package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A topic subscription: routes messages published to {@link #topic} to {@link #taskName}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Subscription {
    public final String topic;

    /** Stable subscription identity; unique per topic. */
    public final String name;

    public final String taskName;
    public final String queue;

    /** Whether the subscription currently receives deliveries (false = paused). */
    public final boolean active;

    /** Whether the registration persists across restarts (false = ephemeral). */
    public final boolean durable;

    @JsonCreator
    public Subscription(
            @JsonProperty("topic") String topic,
            @JsonProperty("subscriptionName") String name,
            @JsonProperty("taskName") String taskName,
            @JsonProperty("queue") String queue,
            @JsonProperty("active") boolean active,
            @JsonProperty("durable") boolean durable) {
        this.topic = topic;
        this.name = name;
        this.taskName = taskName;
        this.queue = queue;
        this.active = active;
        this.durable = durable;
    }
}
