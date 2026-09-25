package org.byteveda.flexiq.worker;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One event sink's counters at one moment, from {@link Worker#eventSinkStats()}.
 *
 * <p>Delivery is at-most-once overall: an event is lost when the sink's buffer
 * is full, when its delivery attempts run out, or when the process dies. Each
 * event that is accepted may still arrive more than once, so receivers dedupe
 * on the CloudEvents {@code id}.
 *
 * @param name the sink's configured name
 * @param kind the sink's {@code kind}, e.g. {@code http}
 * @param delivered events the destination accepted
 * @param droppedBufferFull events dropped because the sink's buffer was full
 * @param droppedRejected events the destination refused outright
 * @param droppedFailed events dropped after every delivery attempt failed
 * @param droppedShutdown events dropped because the worker was stopping
 * @param queued events accepted but not yet delivered or dropped; approximate
 *     while events are in motion
 */
public record EventSinkStats(
        @JsonProperty("name") String name,
        @JsonProperty("kind") String kind,
        @JsonProperty("delivered") long delivered,
        @JsonProperty("dropped_buffer_full") long droppedBufferFull,
        @JsonProperty("dropped_rejected") long droppedRejected,
        @JsonProperty("dropped_failed") long droppedFailed,
        @JsonProperty("dropped_shutdown") long droppedShutdown,
        @JsonProperty("queued") long queued) {}
