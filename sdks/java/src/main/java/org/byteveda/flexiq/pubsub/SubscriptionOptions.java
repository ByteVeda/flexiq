package org.byteveda.flexiq.pubsub;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Options for {@code FlexiQ.subscribe(...)}. The subscription name defaults to
 * the task name, the delivery queue to {@code "default"}, and durability to
 * {@code true} (the registration persists across restarts).
 */
public final class SubscriptionOptions {
    private final @Nullable String name;
    private final String queue;
    private final boolean durable;

    private SubscriptionOptions(Builder b) {
        this.name = b.name;
        this.queue = b.queue;
        this.durable = b.durable;
    }

    /**
     * The defaults: the task's own name, the {@code "default"} queue, durable.
     *
     * @return options with nothing overridden
     */
    public static SubscriptionOptions none() {
        return builder().build();
    }

    /**
     * A builder pre-loaded with those defaults.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The explicit subscription name.
     *
     * @return the name, or {@code null} to default to the task name
     */
    public @Nullable String name() {
        return name;
    }

    /**
     * The queue the subscriber's delivery jobs go to.
     *
     * @return the queue name
     */
    public String queue() {
        return queue;
    }

    /**
     * Whether the registration persists across restarts.
     *
     * @return {@code false} for an ephemeral registration, reaped with its worker
     */
    public boolean durable() {
        return durable;
    }

    /** Collects the overrides for one subscription. */
    public static final class Builder {
        private @Nullable String name;
        private String queue = "default";
        private boolean durable = true;

        /** A builder holding the defaults; reach it through {@link SubscriptionOptions#builder()}. */
        public Builder() {}

        /**
         * Stable subscription identity. Re-registering the same {@code (topic, name)}
         * updates the routing target instead of duplicating the subscription.
         *
         * @param name the subscription's identity within its topic
         * @return {@code this}, for chaining
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * The queue this subscriber's delivery jobs go to.
         *
         * @param queue the queue name; must not be null
         * @return {@code this}, for chaining
         */
        public Builder queue(String queue) {
            this.queue = Objects.requireNonNull(queue, "queue must not be null");
            return this;
        }

        /**
         * {@code false} ties the subscription to one worker process: it registers
         * when that worker starts and is reaped once the worker stops heartbeating.
         *
         * @param durable {@code false} to tie the registration to one worker process
         * @return {@code this}, for chaining
         */
        public Builder durable(boolean durable) {
            this.durable = durable;
            return this;
        }

        /**
         * Freeze the overrides collected so far.
         *
         * @return the immutable options
         */
        public SubscriptionOptions build() {
            return new SubscriptionOptions(this);
        }
    }
}
