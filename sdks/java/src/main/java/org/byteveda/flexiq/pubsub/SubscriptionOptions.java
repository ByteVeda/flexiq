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

    public static SubscriptionOptions none() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The explicit subscription name, or {@code null} to default to the task name. */
    public @Nullable String name() {
        return name;
    }

    /** The queue the subscriber's delivery jobs go to. */
    public String queue() {
        return queue;
    }

    /** Whether the registration persists across restarts; {@code false} = ephemeral. */
    public boolean durable() {
        return durable;
    }

    public static final class Builder {
        private @Nullable String name;
        private String queue = "default";
        private boolean durable = true;

        /**
         * Stable subscription identity. Re-registering the same {@code (topic, name)}
         * updates the routing target instead of duplicating the subscription.
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder queue(String queue) {
            this.queue = Objects.requireNonNull(queue, "queue must not be null");
            return this;
        }

        /**
         * {@code false} ties the subscription to one worker process: it registers
         * when that worker starts and is reaped once the worker stops heartbeating.
         */
        public Builder durable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public SubscriptionOptions build() {
            return new SubscriptionOptions(this);
        }
    }
}
