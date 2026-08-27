package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A topic subscription: routes messages published to {@link #topic} to {@link #taskName}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Subscription {
    /** The topic whose messages fan out to this subscriber. */
    public final String topic;

    /** Stable subscription identity; unique per topic. */
    public final String name;

    /** The task each delivery enqueues. */
    public final String taskName;

    /** The queue those delivery jobs go to. */
    public final String queue;

    /** Whether the subscription currently receives deliveries (false = paused). */
    public final boolean active;

    /** Whether the registration persists across restarts (false = ephemeral). */
    public final boolean durable;

    /**
     * Decoded from the core's JSON subscription row.
     *
     * @param topic the topic whose messages fan out to this subscriber
     * @param name stable subscription identity; unique per topic
     * @param taskName the task each delivery enqueues
     * @param queue the queue those delivery jobs go to
     * @param active whether the subscription currently receives deliveries (false = paused)
     * @param durable whether the registration persists across restarts (false = ephemeral)
     */
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
