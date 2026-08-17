package org.byteveda.flexiq.task;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.serialization.Notes;
import org.jspecify.annotations.Nullable;

/** Immutable per-enqueue options. Unset fields take core defaults. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class EnqueueOptions {
    @JsonProperty("queue")
    private final @Nullable String queue;

    @JsonProperty("priority")
    private final @Nullable Integer priority;

    @JsonProperty("maxRetries")
    private final @Nullable Integer maxRetries;

    @JsonProperty("timeoutMs")
    private final @Nullable Long timeoutMs;

    @JsonProperty("delayMs")
    private final @Nullable Long delayMs;

    @JsonProperty("uniqueKey")
    private final @Nullable String uniqueKey;

    @JsonProperty("metadata")
    private final @Nullable String metadata;

    @JsonProperty("namespace")
    private final @Nullable String namespace;

    @JsonProperty("dependsOn")
    private final @Nullable List<String> dependsOn;

    // Canonical JSON encoding of the structured notes (validated at build time), or null.
    @JsonProperty("notes")
    private final @Nullable String notes;

    // Idempotency inputs resolve to uniqueKey locally (see DefaultFlexiQ) and never cross
    // the wire, so they carry no @JsonProperty and are not serialized into the options JSON.
    private final @Nullable Boolean idempotent;

    private final @Nullable String idempotencyKey;

    private EnqueueOptions(Builder b) {
        this.queue = b.queue;
        this.priority = b.priority;
        this.maxRetries = b.maxRetries;
        this.timeoutMs = b.timeoutMs;
        this.delayMs = b.delayMs;
        this.uniqueKey = b.uniqueKey;
        this.metadata = b.metadata;
        this.namespace = b.namespace;
        this.dependsOn = b.dependsOn;
        this.notes = b.notes;
        this.idempotent = b.idempotent;
        this.idempotencyKey = b.idempotencyKey;
    }

    public static EnqueueOptions none() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A builder seeded with this instance's values, for deriving a modified copy. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.queue = queue;
        b.priority = priority;
        b.maxRetries = maxRetries;
        b.timeoutMs = timeoutMs;
        b.delayMs = delayMs;
        b.uniqueKey = uniqueKey;
        b.metadata = metadata;
        b.namespace = namespace;
        b.dependsOn = dependsOn;
        b.notes = notes;
        b.idempotent = idempotent;
        b.idempotencyKey = idempotencyKey;
        return b;
    }

    /** The target queue, or {@code null} for the default. */
    public @Nullable String queue() {
        return queue;
    }

    /** The job priority, or {@code null} for the core default. */
    public @Nullable Integer priority() {
        return priority;
    }

    /** The retry budget, or {@code null} for the core default. */
    public @Nullable Integer maxRetries() {
        return maxRetries;
    }

    /** The per-job timeout in milliseconds, or {@code null} for the core default. */
    public @Nullable Long timeoutMs() {
        return timeoutMs;
    }

    /** Job ids this enqueue waits on before it can be dequeued, or {@code null} when none. */
    public @Nullable List<String> dependsOn() {
        return dependsOn;
    }

    /** The explicit dedup key, or {@code null} when none was set. */
    public @Nullable String uniqueKey() {
        return uniqueKey;
    }

    /**
     * Tri-state idempotency toggle: {@code TRUE} forces auto-derivation of a {@code uniqueKey},
     * {@code FALSE} opts this enqueue out of a task-level default, {@code null} defers to the task.
     */
    public @Nullable Boolean idempotent() {
        return idempotent;
    }

    /** An explicit idempotency key (used as the {@code uniqueKey} when set), or {@code null}. */
    public @Nullable String idempotencyKey() {
        return idempotencyKey;
    }

    public static final class Builder {
        private @Nullable String queue;
        private @Nullable Integer priority;
        private @Nullable Integer maxRetries;
        private @Nullable Long timeoutMs;
        private @Nullable Long delayMs;
        private @Nullable String uniqueKey;
        private @Nullable String metadata;
        private @Nullable String namespace;
        private @Nullable List<String> dependsOn;
        private @Nullable String notes;
        private @Nullable Boolean idempotent;
        private @Nullable String idempotencyKey;

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
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

        public Builder delayMs(long delayMs) {
            if (delayMs < 0) {
                throw new IllegalArgumentException("delayMs must be >= 0");
            }
            this.delayMs = delayMs;
            return this;
        }

        /** Schedule the job after {@code delay} (Duration form of {@link #delayMs}). */
        public Builder delay(Duration delay) {
            this.delayMs = delay.toMillis();
            return this;
        }

        /** Per-job timeout (Duration form of {@link #timeoutMs}). */
        public Builder timeout(Duration timeout) {
            this.timeoutMs = timeout.toMillis();
            return this;
        }

        /** Idempotency key — alias of {@link #uniqueKey} in the guide's vocabulary. */
        public Builder jobId(String jobId) {
            this.uniqueKey = jobId;
            return this;
        }

        public Builder uniqueKey(String uniqueKey) {
            this.uniqueKey = uniqueKey;
            return this;
        }

        public Builder metadata(String metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * Gate this job on the completion of the given job ids: it stays pending (not dequeued)
         * until every dependency completes, and is cancelled if any dependency fails. Each id
         * must reference a job that is still live or already complete.
         */
        public Builder dependsOn(String... jobIds) {
            this.dependsOn = List.of(jobIds);
            return this;
        }

        /** List form of {@link #dependsOn(String...)}. */
        public Builder dependsOn(List<String> jobIds) {
            this.dependsOn = List.copyOf(jobIds);
            return this;
        }

        /**
         * Attach a bounded, user-readable annotation map to the job (validated and canonically
         * encoded now, so a contract violation fails fast). Distinct from the opaque
         * {@link #metadata} blob. Passing {@code null} clears any previously set notes.
         *
         * @throws org.byteveda.flexiq.errors.NotesValidationException if the map breaks the
         *     {@link Notes} contract (field/key/value/depth/size limits)
         */
        public Builder notes(Map<String, ?> notes) {
            this.notes = Notes.encode(notes);
            return this;
        }

        /**
         * Dedupe this enqueue by auto-deriving a {@code uniqueKey} from the task name and
         * payload. A duplicate enqueue is a no-op while the first job is pending or running.
         * An explicit {@link #uniqueKey}/{@link #idempotencyKey} takes precedence; passing
         * {@code false} opts out of a task-level default.
         */
        public Builder idempotent(boolean idempotent) {
            this.idempotent = idempotent;
            return this;
        }

        /** Dedupe this enqueue under an explicit key (equivalent to a caller-supplied {@code uniqueKey}). */
        public Builder idempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return this;
        }

        public EnqueueOptions build() {
            return new EnqueueOptions(this);
        }
    }
}
