package org.byteveda.flexiq.pubsub;

import org.jspecify.annotations.Nullable;

/**
 * A resolved subscription declaration recorded by {@code FlexiQ.subscribe(...)}.
 * Workers register these at start (ephemeral entries bind to the started
 * worker's id), and {@code publish(...)} reads the task delivery defaults so
 * deliveries honor each subscriber's own settings.
 */
public final class SubscriptionConfig {
    private final String topic;
    private final String name;
    private final String taskName;
    private final String queue;
    private final boolean durable;
    private final @Nullable Integer taskPriority;
    private final @Nullable Integer taskMaxRetries;
    private final @Nullable Long taskTimeoutMs;

    /**
     * A resolved declaration, built by {@code FlexiQ.subscribe(...)}.
     *
     * @param topic the topic whose messages fan out to this subscriber
     * @param name the subscription's identity; re-registering the same
     *     {@code (topic, name)} retargets it instead of duplicating it
     * @param taskName the task each delivery enqueues
     * @param queue the queue those delivery jobs go to
     * @param durable {@code false} ties the registration to one worker process and
     *     reaps it once that worker stops heartbeating
     * @param taskPriority the subscriber task's own default priority, or {@code null}
     *     for the core default
     * @param taskMaxRetries the subscriber task's own default retry budget, or
     *     {@code null} for the core default
     * @param taskTimeoutMs the subscriber task's own default timeout, or {@code null}
     *     for the core default
     */
    public SubscriptionConfig(
            String topic,
            String name,
            String taskName,
            String queue,
            boolean durable,
            @Nullable Integer taskPriority,
            @Nullable Integer taskMaxRetries,
            @Nullable Long taskTimeoutMs) {
        this.topic = topic;
        this.name = name;
        this.taskName = taskName;
        this.queue = queue;
        this.durable = durable;
        this.taskPriority = taskPriority;
        this.taskMaxRetries = taskMaxRetries;
        this.taskTimeoutMs = taskTimeoutMs;
    }

    /**
     * The topic whose messages fan out to this subscriber.
     *
     * @return the topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * The subscription's identity within its topic.
     *
     * @return the subscription name
     */
    public String name() {
        return name;
    }

    /**
     * The task each delivery enqueues.
     *
     * @return the task name
     */
    public String taskName() {
        return taskName;
    }

    /**
     * The queue the delivery jobs go to.
     *
     * @return the queue name
     */
    public String queue() {
        return queue;
    }

    /**
     * Whether the registration outlives the worker that made it.
     *
     * @return {@code false} for an ephemeral registration, reaped with its worker
     */
    public boolean durable() {
        return durable;
    }

    /**
     * The subscriber task's default priority.
     *
     * @return the priority, or {@code null} for the core default
     */
    public @Nullable Integer taskPriority() {
        return taskPriority;
    }

    /**
     * The subscriber task's default retry budget.
     *
     * @return the budget, or {@code null} for the core default
     */
    public @Nullable Integer taskMaxRetries() {
        return taskMaxRetries;
    }

    /**
     * The subscriber task's default timeout.
     *
     * @return the timeout in milliseconds, or {@code null} for the core default
     */
    public @Nullable Long taskTimeoutMs() {
        return taskTimeoutMs;
    }
}
