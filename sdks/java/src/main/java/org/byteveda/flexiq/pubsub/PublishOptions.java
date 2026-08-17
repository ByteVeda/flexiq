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

    public static PublishOptions none() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The per-subscriber dedup key, or {@code null} when the publish is unkeyed. */
    public @Nullable String idempotencyKey() {
        return idempotencyKey;
    }

    /** The opaque metadata blob every delivery carries, or {@code null}. */
    public @Nullable String metadata() {
        return metadata;
    }

    /** Canonical notes JSON (validated at build time), or {@code null}. */
    public @Nullable String notes() {
        return notes;
    }

    public @Nullable Integer priority() {
        return priority;
    }

    public @Nullable Long delayMs() {
        return delayMs;
    }

    public @Nullable Integer maxRetries() {
        return maxRetries;
    }

    public @Nullable Long timeoutMs() {
        return timeoutMs;
    }

    public @Nullable Long expiresMs() {
        return expiresMs;
    }

    public @Nullable Long resultTtlMs() {
        return resultTtlMs;
    }

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

        /**
         * Dedupe per subscriber: republishing the same key yields no new deliveries,
         * while a subscription added later still gets its own copy.
         */
        public Builder idempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return this;
        }

        public Builder metadata(String metadata) {
            this.metadata = metadata;
            return this;
        }

        /**
         * Attach a bounded, user-readable annotation map to every delivery
         * (validated and canonically encoded now). Each delivery additionally
         * carries {@code topic} and {@code subscription} keys.
         *
         * @throws org.byteveda.flexiq.errors.NotesValidationException if the map
         *     breaks the {@link Notes} contract
         */
        public Builder notes(Map<String, ?> notes) {
            this.notes = Notes.encode(notes);
            return this;
        }

        /** Override every delivery's priority, beating the subscriber tasks' own defaults. */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder delayMs(long delayMs) {
            if (delayMs < 0) {
                throw new IllegalArgumentException("delayMs must be >= 0");
            }
            this.delayMs = delayMs;
            return this;
        }

        /** Schedule the deliveries after {@code delay} (Duration form of {@link #delayMs}). */
        public Builder delay(Duration delay) {
            return delayMs(delay.toMillis());
        }

        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be >= 0");
            }
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder timeoutMs(long timeoutMs) {
            if (timeoutMs < 0) {
                throw new IllegalArgumentException("timeoutMs must be >= 0");
            }
            this.timeoutMs = timeoutMs;
            return this;
        }

        /** Per-delivery timeout (Duration form of {@link #timeoutMs}). */
        public Builder timeout(Duration timeout) {
            return timeoutMs(timeout.toMillis());
        }

        /** Deliveries not started within this window are discarded. */
        public Builder expiresMs(long expiresMs) {
            if (expiresMs < 0) {
                throw new IllegalArgumentException("expiresMs must be >= 0");
            }
            this.expiresMs = expiresMs;
            return this;
        }

        /** Expiry window (Duration form of {@link #expiresMs}). */
        public Builder expires(Duration expires) {
            return expiresMs(expires.toMillis());
        }

        /** How long each delivery's result is retained after completion. */
        public Builder resultTtlMs(long resultTtlMs) {
            if (resultTtlMs < 0) {
                throw new IllegalArgumentException("resultTtlMs must be >= 0");
            }
            this.resultTtlMs = resultTtlMs;
            return this;
        }

        /** Result retention (Duration form of {@link #resultTtlMs}). */
        public Builder resultTtl(Duration resultTtl) {
            return resultTtlMs(resultTtl.toMillis());
        }

        public PublishOptions build() {
            return new PublishOptions(this);
        }
    }
}
