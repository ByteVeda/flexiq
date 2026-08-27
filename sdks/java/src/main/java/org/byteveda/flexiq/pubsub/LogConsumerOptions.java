package org.byteveda.flexiq.pubsub;

/**
 * Options for {@code FlexiQ.logConsumer(...)}. The poll interval defaults to one
 * second, the batch size to 100, and the error policy to {@code "retry"}.
 */
public final class LogConsumerOptions {
    private final long pollIntervalMs;
    private final int batchSize;
    private final String onError;

    private LogConsumerOptions(Builder b) {
        this.pollIntervalMs = b.pollIntervalMs;
        this.batchSize = b.batchSize;
        this.onError = b.onError;
    }

    /**
     * The defaults: a one-second poll, batches of 100, {@code "retry"} on error.
     *
     * @return options with nothing overridden
     */
    public static LogConsumerOptions none() {
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
     * Milliseconds to wait after an empty poll before re-reading.
     *
     * @return the interval in milliseconds
     */
    public long pollIntervalMs() {
        return pollIntervalMs;
    }

    /**
     * Max messages pulled per poll.
     *
     * @return the batch ceiling
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * The error policy: {@code "retry"} (default) or {@code "skip"}.
     *
     * @return the policy
     */
    public String onError() {
        return onError;
    }

    /** Collects the overrides for one managed consumer. */
    public static final class Builder {
        private long pollIntervalMs = 1000;
        private int batchSize = 100;
        private String onError = "retry";

        /** A builder holding the defaults; reach it through {@link LogConsumerOptions#builder()}. */
        public Builder() {}

        /**
         * How long to wait after an empty poll before re-reading.
         *
         * @param pollIntervalMs the interval in milliseconds; must be positive
         * @return {@code this}, for chaining
         */
        public Builder pollIntervalMs(long pollIntervalMs) {
            if (pollIntervalMs <= 0) {
                throw new IllegalArgumentException("pollIntervalMs must be positive");
            }
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        /**
         * The most messages one poll pulls.
         *
         * @param batchSize the batch ceiling; must be positive
         * @return {@code this}, for chaining
         */
        public Builder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive");
            }
            this.batchSize = batchSize;
            return this;
        }

        /**
         * {@code "retry"} leaves a failed message un-acked so the batch re-reads next
         * poll; {@code "skip"} acks past it and continues. Rejects any other value.
         *
         * @param onError {@code "retry"} or {@code "skip"}
         * @return {@code this}, for chaining
         */
        public Builder onError(String onError) {
            if (!"retry".equals(onError) && !"skip".equals(onError)) {
                throw new IllegalArgumentException("onError must be 'retry' or 'skip'");
            }
            this.onError = onError;
            return this;
        }

        /**
         * Freeze the overrides collected so far.
         *
         * @return the immutable options
         */
        public LogConsumerOptions build() {
            return new LogConsumerOptions(this);
        }
    }
}
