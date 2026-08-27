package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Backlog snapshot for one topic subscription: how much of its fan-out is outstanding. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TopicStat {
    /** The topic being fanned out. */
    public final String topic;

    /** Stable subscription identity; unique per topic. */
    public final String subscription;

    /** Task enqueued for each published message. */
    public final String taskName;

    /** Queue deliveries are enqueued into. */
    public final String queue;

    /** Whether the subscription currently receives deliveries (false = paused). */
    public final boolean active;

    /** Whether the registration persists across restarts (false = ephemeral). */
    public final boolean durable;

    /** Deliveries waiting to run. */
    public final long pending;

    /** Deliveries currently executing. */
    public final long running;

    /** Deliveries in the dead-letter queue. */
    public final long dead;

    /** Age (ms) of the oldest still-pending delivery, or {@code null} at zero backlog. */
    public final Long oldestPendingAgeMs;

    /**
     * Decoded from the core's JSON backlog view.
     *
     * @param topic the topic being fanned out
     * @param subscription stable subscription identity; unique per topic
     * @param taskName task enqueued for each published message
     * @param queue queue deliveries are enqueued into
     * @param active whether the subscription currently receives deliveries (false = paused)
     * @param durable whether the registration persists across restarts (false = ephemeral)
     * @param pending deliveries waiting to run
     * @param running deliveries currently executing
     * @param dead deliveries in the dead-letter queue
     * @param oldestPendingAgeMs age (ms) of the oldest still-pending delivery, or {@code null} at zero backlog
     */
    @JsonCreator
    public TopicStat(
            @JsonProperty("topic") String topic,
            @JsonProperty("subscription") String subscription,
            @JsonProperty("taskName") String taskName,
            @JsonProperty("queue") String queue,
            @JsonProperty("active") boolean active,
            @JsonProperty("durable") boolean durable,
            @JsonProperty("pending") long pending,
            @JsonProperty("running") long running,
            @JsonProperty("dead") long dead,
            @JsonProperty("oldestPendingAgeMs") Long oldestPendingAgeMs) {
        this.topic = topic;
        this.subscription = subscription;
        this.taskName = taskName;
        this.queue = queue;
        this.active = active;
        this.durable = durable;
        this.pending = pending;
        this.running = running;
        this.dead = dead;
        this.oldestPendingAgeMs = oldestPendingAgeMs;
    }
}
