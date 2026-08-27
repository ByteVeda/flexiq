package org.byteveda.flexiq.pubsub;

import java.lang.reflect.Type;
import java.util.function.Consumer;

/**
 * A resolved managed-consumer declaration recorded by {@code FlexiQ.logConsumer(...)}.
 * The worker spawns one poll loop per config at start: it pulls the topic's stored
 * messages, decodes each to {@link #payloadType()}, invokes {@link #handler()}, and
 * advances the log cursor.
 */
public final class LogConsumerConfig {
    private final String topic;
    private final String name;
    private final Type payloadType;
    private final Consumer<Object> handler;
    private final long pollIntervalMs;
    private final int batchSize;
    private final String onError;

    /**
     * A resolved declaration, built by {@code FlexiQ.logConsumer(...)}.
     *
     * @param topic the log the poll loop reads from
     * @param name the cursor's identity, so two consumers of one topic advance independently
     * @param payloadType the type each stored message decodes to
     * @param handler what runs per decoded message
     * @param pollIntervalMs how long to wait after an empty poll before re-reading
     * @param batchSize the most messages one poll pulls
     * @param onError {@code "retry"} to re-read a failed message, {@code "skip"} to ack past it
     */
    public LogConsumerConfig(
            String topic,
            String name,
            Type payloadType,
            Consumer<Object> handler,
            long pollIntervalMs,
            int batchSize,
            String onError) {
        this.topic = topic;
        this.name = name;
        this.payloadType = payloadType;
        this.handler = handler;
        this.pollIntervalMs = pollIntervalMs;
        this.batchSize = batchSize;
        this.onError = onError;
    }

    /**
     * The log this consumer reads.
     *
     * @return the topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * The cursor's identity, so two consumers of one topic advance independently.
     *
     * @return the consumer name
     */
    public String name() {
        return name;
    }

    /**
     * The type each message payload decodes to before it reaches the handler.
     *
     * @return the payload type, generic arguments included
     */
    public Type payloadType() {
        return payloadType;
    }

    /**
     * The handler invoked per decoded message.
     *
     * @return the handler the poll loop calls
     */
    public Consumer<Object> handler() {
        return handler;
    }

    /**
     * How long the loop waits after an empty poll before re-reading.
     *
     * @return the interval in milliseconds
     */
    public long pollIntervalMs() {
        return pollIntervalMs;
    }

    /**
     * The most messages one poll pulls.
     *
     * @return the batch ceiling
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * {@code "retry"} re-reads a failed message; {@code "skip"} acks past it.
     *
     * @return the error policy
     */
    public String onError() {
        return onError;
    }
}
