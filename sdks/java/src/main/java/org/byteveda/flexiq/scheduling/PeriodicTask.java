package org.byteveda.flexiq.scheduling;

import org.jspecify.annotations.Nullable;

/** A cron-scheduled task registration. The worker enqueues it when due. */
public final class PeriodicTask {
    /** The schedule's own identity; re-registering the same name updates it. */
    public final String name;

    /** The task enqueued on each firing. */
    public final String taskName;

    /** The cron expression deciding when it fires. */
    public final String cron;

    /** The payload each firing enqueues, or {@code null} for none. */
    public final @Nullable Object payload;

    /** The queue the jobs go to, or {@code null} for the default queue. */
    public final @Nullable String queue;

    /** The zone the cron expression is read in, or {@code null} for UTC. */
    public final @Nullable String timezone;

    /** Whether the schedule fires; a disabled one keeps its registration. */
    public final boolean enabled;

    private PeriodicTask(Builder b) {
        this.name = b.name;
        this.taskName = b.taskName;
        this.cron = b.cron;
        this.payload = b.payload;
        this.queue = b.queue;
        this.timezone = b.timezone;
        this.enabled = b.enabled;
    }

    /**
     * Start a registration; everything else has a default.
     *
     * @param name the schedule's own identity
     * @param taskName the task enqueued on each firing
     * @param cron the expression deciding when it fires
     * @return the builder
     */
    public static Builder builder(String name, String taskName, String cron) {
        return new Builder(name, taskName, cron);
    }

    /** Collects the optional parts of a registration. */
    public static final class Builder {
        private final String name;
        private final String taskName;
        private final String cron;
        private @Nullable Object payload;
        private @Nullable String queue;
        private @Nullable String timezone;
        private boolean enabled = true;

        Builder(String name, String taskName, String cron) {
            this.name = name;
            this.taskName = taskName;
            this.cron = cron;
        }

        /**
         * The payload each firing enqueues.
         *
         * @param payload the value, serialized like any other; {@code null} for none
         * @return {@code this}, for chaining
         */
        public Builder payload(@Nullable Object payload) {
            this.payload = payload;
            return this;
        }

        /**
         * The queue the jobs go to.
         *
         * @param queue the queue name, or {@code null} for the default queue
         * @return {@code this}, for chaining
         */
        public Builder queue(@Nullable String queue) {
            this.queue = queue;
            return this;
        }

        /**
         * The zone the cron expression is read in.
         *
         * @param timezone an IANA zone id, or {@code null} for UTC
         * @return {@code this}, for chaining
         */
        public Builder timezone(@Nullable String timezone) {
            this.timezone = timezone;
            return this;
        }

        /**
         * Whether the schedule fires. Defaults to {@code true}.
         *
         * @param enabled {@code false} to keep the registration but stop firing
         * @return {@code this}, for chaining
         */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Freeze the registration.
         *
         * @return the immutable schedule
         */
        public PeriodicTask build() {
            return new PeriodicTask(this);
        }
    }
}
