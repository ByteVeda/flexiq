package org.byteveda.flexiq.pubsub;

import java.time.Duration;
import java.util.Map;
import org.byteveda.flexiq.serialization.Notes;
import org.jspecify.annotations.Nullable;

/**
 * Options for {@code FlexiQ.publish(...)}. Every field is optional; unset
 * delivery settings resolve per subscriber (the subscriber task's own defaults,
 * then the core defaults). All durations are milliseconds.
 */
public final class PublishOptions {
    private final @Nullable String idempotencyKey;
    private final @Nullable String metadata;
    private final @Nullable String notes;
    private final @Nullable Integer priority;
    private final @Nullable Long delayMs;
    private final @Nullable Integer maxRetries;
    private final @Nullable Long timeoutMs;
    private final @Nullable Long expiresMs;
    private final @Nullable Long resultTtlMs;

    private PublishOptions(Builder b) {
        this.idempotencyKey = b.idempotencyKey;
        this.metadata = b.metadata;
        this.notes = b.notes;
        this.priority = b.priority;
        this.delayMs = b.delayMs;
        this.maxRetries = b.maxRetries;
        this.timeoutMs = b.timeoutMs;
        this.expiresMs = b.expiresMs;
        this.resultTtlMs = b.resultTtlMs;
    }

    /**
     * Nothing overridden: every delivery resolves its settings per subscriber.
     *
     * @return options with no field set
     */
    public static PublishOptions none() {
        return builder().build();
    }

    /**
     * A builder for a publish.
     *
     * @return an empty builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The per-subscriber dedup key.
     *
     * @return the key, or {@code null} when the publish is unkeyed
     */
    public @Nullable String idempotencyKey() {
        return idempotencyKey;
    }

    /**
     * The opaque metadata blob every delivery carries.
     *
     * @return the blob, or {@code null} when none was attached
     */
    public @Nullable String metadata() {
        return metadata;
    }

    /**
     * Canonical notes JSON, validated at build time.
     *
     * @return the encoded notes, or {@code null} when none were attached
     */
    public @Nullable String notes() {
        return notes;
    }

    /**
     * The priority forced on every delivery.
     *
     * @return the override, or {@code null} to leave each subscriber task's own default
     */
    public @Nullable Integer priority() {
        return priority;
    }

    /**
     * How long each delivery waits before it becomes runnable.
     *
     * @return the delay in milliseconds, or {@code null} to deliver immediately
     */
    public @Nullable Long delayMs() {
        return delayMs;
    }

    /**
     * The retry ceiling forced on every delivery.
     *
     * @return the override, or {@code null} to leave each subscriber task's own default
     */
    public @Nullable Integer maxRetries() {
        return maxRetries;
    }

    /**
     * The per-delivery execution timeout.
     *
     * @return the timeout in milliseconds, or {@code null} to leave each
     *     subscriber task's own default
     */
    public @Nullable Long timeoutMs() {
        return timeoutMs;
    }

    /**
     * The window a delivery must start within before it is discarded.
     *
     * @return the window in milliseconds, or {@code null} for no expiry
     */
    public @Nullable Long expiresMs() {
        return expiresMs;
    }

    /**
     * How long each delivery's result is retained after it completes.
     *
     * @return the retention in milliseconds, or {@code null} to leave each
     *     subscriber task's own default
     */
    public @Nullable Long resultTtlMs() {
        return resultTtlMs;
    }

    /** Collects the overrides for one publish. */
    public static final class Builder {
        private @Nullable String idempotencyKey;
        private @Nullable String metadata;
        private @Nullable String notes;
        private @Nullable Integer priority;
        private @Nullable Long delayMs;
        private @Nullable Integer maxRetries;
        private @Nullable Long timeoutMs;
        private @Nullable Long expiresMs;
        private @Nullable Long resultTtlMs;

        /** An empty builder; reach it through {@link PublishOptions#builder()}. */
        public Builder() {}

        /**
         * Dedupe per subscriber: republishing the same key yields no new deliveries,
         * while a subscription added later still gets its own copy.
         *
         * @param idempotencyKey what makes this publish the same publish — an event
         *     id, not a timestamp
         * @return {@code this}, for chaining
         */
        public Builder idempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return this;
        }

        /**
         * Attach an opaque blob the SDK never parses, carried on every delivery.
         *
         * @param metadata the blob, for consumers of the delivery to interpret
         * @return {@code this}, for chaining
         */
        public Builder metadata(String metadata) {
            this.metadata = metadata;
            return this;
        }

        /**
         * Attach a bounded, user-readable annotation map to every delivery
         * (validated and canonically encoded now). Each delivery additionally
         * carries {@code topic} and {@code subscription} keys.
         *
         * @param notes the annotations, encoded canonically so equal maps produce
         *     equal bytes
         * @return {@code this}, for chaining
         * @throws org.byteveda.flexiq.errors.NotesValidationException if the map
         *     breaks the {@link Notes} contract
         */
        public Builder notes(Map<String, ?> notes) {
            this.notes = Notes.encode(notes);
            return this;
        }

        /**
         * Override every delivery's priority, beating the subscriber tasks' own defaults.
         *
         * @param priority the priority to run every delivery at
         * @return {@code this}, for chaining
         */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Hold every delivery back for {@code delayMs} before it becomes runnable.
         *
         * @param delayMs the delay in milliseconds; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder delayMs(long delayMs) {
            if (delayMs < 0) {
                throw new IllegalArgumentException("delayMs must be >= 0");
            }
            this.delayMs = delayMs;
            return this;
        }

        /**
         * Schedule the deliveries after {@code delay} (Duration form of {@link #delayMs}).
         *
         * @param delay the delay; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder delay(Duration delay) {
            return delayMs(delay.toMillis());
        }

        /**
         * Cap every delivery's retries, beating the subscriber tasks' own defaults.
         *
         * @param maxRetries the ceiling; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be >= 0");
            }
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Bound how long one delivery may run before it is failed as timed out.
         *
         * @param timeoutMs the timeout in milliseconds; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder timeoutMs(long timeoutMs) {
            if (timeoutMs < 0) {
                throw new IllegalArgumentException("timeoutMs must be >= 0");
            }
            this.timeoutMs = timeoutMs;
            return this;
        }

        /**
         * Per-delivery timeout (Duration form of {@link #timeoutMs}).
         *
         * @param timeout the timeout; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder timeout(Duration timeout) {
            return timeoutMs(timeout.toMillis());
        }

        /**
         * Deliveries not started within this window are discarded.
         *
         * @param expiresMs the window in milliseconds; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder expiresMs(long expiresMs) {
            if (expiresMs < 0) {
                throw new IllegalArgumentException("expiresMs must be >= 0");
            }
            this.expiresMs = expiresMs;
            return this;
        }

        /**
         * Expiry window (Duration form of {@link #expiresMs}).
         *
         * @param expires the window; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder expires(Duration expires) {
            return expiresMs(expires.toMillis());
        }

        /**
         * How long each delivery's result is retained after completion.
         *
         * @param resultTtlMs the retention in milliseconds; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder resultTtlMs(long resultTtlMs) {
            if (resultTtlMs < 0) {
                throw new IllegalArgumentException("resultTtlMs must be >= 0");
            }
            this.resultTtlMs = resultTtlMs;
            return this;
        }

        /**
         * Result retention (Duration form of {@link #resultTtlMs}).
         *
         * @param resultTtl the retention; must not be negative
         * @return {@code this}, for chaining
         */
        public Builder resultTtl(Duration resultTtl) {
            return resultTtlMs(resultTtl.toMillis());
        }

        /**
         * Freeze the overrides collected so far.
         *
         * @return the immutable options
         */
        public PublishOptions build() {
            return new PublishOptions(this);
        }
    }
}
