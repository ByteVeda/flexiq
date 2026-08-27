package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Lag snapshot for one log subscription: how far its cursor trails the topic. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TopicLogStat {
    /** The log being read. */
    public final String topic;

    /** The consumer whose cursor this describes. */
    public final String subscription;

    /** Current read cursor (last-acked id), or {@code null} if nothing acked yet. */
    public final String cursor;

    /** Number of messages after the cursor still to be consumed. */
    public final long lag;

    /** Age (ms) of the oldest un-acked message, or {@code null} when caught up. */
    public final Long oldestUnackedAgeMs;

    /**
     * Decoded from the core's JSON lag view.
     *
     * @param topic the log being read
     * @param subscription the consumer whose cursor this describes
     * @param cursor current read cursor (last-acked id), or {@code null} if nothing acked yet
     * @param lag number of messages after the cursor still to be consumed
     * @param oldestUnackedAgeMs age (ms) of the oldest un-acked message, or {@code null} when caught up
     */
    @JsonCreator
    public TopicLogStat(
            @JsonProperty("topic") String topic,
            @JsonProperty("subscription") String subscription,
            @JsonProperty("cursor") String cursor,
            @JsonProperty("lag") long lag,
            @JsonProperty("oldestUnackedAgeMs") Long oldestUnackedAgeMs) {
        this.topic = topic;
        this.subscription = subscription;
        this.cursor = cursor;
        this.lag = lag;
        this.oldestUnackedAgeMs = oldestUnackedAgeMs;
    }
}
