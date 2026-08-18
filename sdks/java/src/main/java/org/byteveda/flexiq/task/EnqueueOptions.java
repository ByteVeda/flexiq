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

    // The debounce window. Present alongside the other two wire fields, it routes the
    // enqueue through the core's enqueue_debounced instead of a plain insert.
    @JsonProperty("debounceWindowMs")
    private final @Nullable Long debounceWindowMs;

    @JsonProperty("debounceMaxWaitMs")
    private final @Nullable Long debounceMaxWaitMs;

    @JsonProperty("debounceReplacePayload")
    private final @Nullable Boolean debounceReplacePayload;

    // Serialized as the wire's debounceKey, but what a caller sets here is a *template*
    // ("report:{userId}"). DefaultFlexiQ resolves it against the payload and re-sets it
    // before encoding, so only a concrete key ever crosses the boundary.
    @JsonProperty("debounceKey")
    private final @Nullable String debounceKey;

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
        this.debounceWindowMs = b.debounceWindowMs;
        this.debounceMaxWaitMs = b.debounceMaxWaitMs;
        this.debounceReplacePayload = b.debounceReplacePayload;
        this.debounceKey = b.debounceKey;
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
        b.debounceWindowMs = debounceWindowMs;
        b.debounceMaxWaitMs = debounceMaxWaitMs;
        b.debounceReplacePayload = debounceReplacePayload;
        b.debounceKey = debounceKey;
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

    /** The enqueue delay in milliseconds, or {@code null} when the job runs as soon as it can. */
    public @Nullable Long delayMs() {
        return delayMs;
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

    /** The debounce window in milliseconds, or {@code null} when this enqueue does not debounce. */
    public @Nullable Long debounceWindowMs() {
        return debounceWindowMs;
    }

    /** The ceiling on a debounced job's total delay, in milliseconds, or {@code null}. */
    public @Nullable Long debounceMaxWaitMs() {
        return debounceMaxWaitMs;
    }

    /** Whether a repeat debounced enqueue overwrites the pending job's payload. */
    public boolean debounceReplacePayload() {
        return Boolean.TRUE.equals(debounceReplacePayload);
    }

    /**
     * The debounce key template (e.g. {@code "report:{userId}"}), or {@code null} when this
     * enqueue does not debounce. Resolved against the payload at enqueue time.
     */
    public @Nullable String debounceKey() {
        return debounceKey;
    }

    /** Whether this enqueue debounces — i.e. whether a window was set. */
    public boolean debounces() {
        return debounceWindowMs != null;
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
        private @Nullable Long debounceWindowMs;
        private @Nullable Long debounceMaxWaitMs;
        private @Nullable Boolean debounceReplacePayload;
        private @Nullable String debounceKey;

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

        /**
         * Collapse a burst of enqueues that share a {@link #debounceKey} into one run:
         * while the pending job is unclaimed, each further enqueue slides its deadline
         * {@code window} into the future instead of inserting a second job. Distinct
         * from {@link #idempotent}, which dedupes onto the first job and never moves it.
         *
         * <p>Setting this turns debouncing on, and so requires both {@link #debounceKey}
         * and {@link #debounceMaxWait} — {@link #build()} rejects an incomplete set.
         */
        public Builder debounce(Duration window) {
            this.debounceWindowMs = window.toMillis();
            return this;
        }

        /**
         * The window's identity, as a template resolved against the payload:
         * {@code "report:{userId}"} reads the {@code userId} property off the enqueued
         * payload, and a dotted path ({@code "{owner.id}"}) walks into a nested object.
         * A placeholder the payload does not provide throws at enqueue rather than
         * degrading to a key every caller shares. A template with no placeholder is a
         * deliberate single window for the task.
         */
        public Builder debounceKey(String debounceKey) {
            this.debounceKey = debounceKey;
            return this;
        }

        /**
         * Ceiling on the total delay, measured from when the window opened, and never
         * shorter than {@link #debounce}. Mandatory: without it a caller who never stops
         * enqueuing starves the job forever, which is the classic debounce footgun.
         */
        public Builder debounceMaxWait(Duration maxWait) {
            this.debounceMaxWaitMs = maxWait.toMillis();
            return this;
        }

        /**
         * Whether an enqueue landing on an open window also overwrites the pending job's
         * payload with its own. The default {@code false} keeps the payload the window
         * opened with — a repeat enqueue is a vote to run again soon, not a redefinition
         * of the run.
         */
        public Builder debounceReplacePayload(boolean debounceReplacePayload) {
            this.debounceReplacePayload = debounceReplacePayload;
            return this;
        }

        /**
         * @throws IllegalArgumentException if the debounce options are incomplete or
         *     contradictory — a window with no key or no max wait, a max wait shorter
         *     than the window, or any debounce field set without a window
         */
        public EnqueueOptions build() {
            validateDebounce();
            return new EnqueueOptions(this);
        }

        /**
         * The four debounce fields are a set, not four independent knobs, so every
         * incomplete combination is refused here rather than half-applied at enqueue.
         */
        private void validateDebounce() {
            if (debounceWindowMs == null) {
                if (debounceKey != null || debounceMaxWaitMs != null || debounceReplacePayload != null) {
                    throw new IllegalArgumentException(
                            "debounceKey/debounceMaxWait/debounceReplacePayload require debounce(...)"
                                    + " — the window length is what turns debouncing on");
                }
                return;
            }
            if (debounceWindowMs <= 0) {
                throw new IllegalArgumentException(
                        "debounce must be a positive duration, got " + debounceWindowMs + "ms");
            }
            if (debounceMaxWaitMs == null) {
                throw new IllegalArgumentException("debounce(...) requires debounceMaxWait(...) — an unbounded"
                        + " debounce starves the job while enqueues keep arriving");
            }
            if (debounceKey == null || debounceKey.isEmpty()) {
                throw new IllegalArgumentException("debounce(...) requires a non-empty debounceKey(...) — one window"
                        + " per task would collapse every caller's work into a single run, so the key is what"
                        + " scopes it, e.g. \"report:{userId}\"");
            }
            if (debounceMaxWaitMs < debounceWindowMs) {
                throw new IllegalArgumentException("debounceMaxWait (" + debounceMaxWaitMs + "ms) must be at least"
                        + " as long as debounce (" + debounceWindowMs + "ms), or the window never gets to slide");
            }
        }
    }
}
