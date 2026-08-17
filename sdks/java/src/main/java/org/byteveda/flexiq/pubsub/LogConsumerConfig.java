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

    public String topic() {
        return topic;
    }

    public String name() {
        return name;
    }

    /** The type each message payload decodes to before it reaches the handler. */
    public Type payloadType() {
        return payloadType;
    }

    /** The handler invoked per decoded message. */
    public Consumer<Object> handler() {
        return handler;
    }

    public long pollIntervalMs() {
        return pollIntervalMs;
    }

    public int batchSize() {
        return batchSize;
    }

    /** {@code "retry"} re-reads a failed message; {@code "skip"} acks past it. */
    public String onError() {
        return onError;
    }
}
