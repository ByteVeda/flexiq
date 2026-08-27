package org.byteveda.flexiq.predicates;

import java.time.Duration;
import java.time.Instant;

/**
 * The outcome of an {@link EnqueueGate}: allow the enqueue, silently skip it,
 * defer it by a delay, or reject it with a reason. Build with the static
 * factories and pattern-match the variants where decisions are honored.
 */
public sealed interface EnqueueDecision
        permits EnqueueDecision.Allow, EnqueueDecision.Skip, EnqueueDecision.Defer, EnqueueDecision.Reject {

    /** Proceed with the enqueue unchanged. */
    record Allow() implements EnqueueDecision {}

    /**
     * Do not enqueue: {@code tryEnqueue} returns empty, {@code enqueue} throws.
     *
     * @param reason why the enqueue was skipped
     */
    record Skip(String reason) implements EnqueueDecision {}

    /**
     * Enqueue, but delayed by {@code delay} (overrides any delay in the passed options).
     *
     * @param delay how long to hold the job back; never negative
     */
    record Defer(Duration delay) implements EnqueueDecision {
        public Defer {
            if (delay == null || delay.isNegative()) {
                throw new IllegalArgumentException("defer delay must be non-negative");
            }
        }
    }

    /**
     * Refuse the enqueue: both {@code enqueue} and {@code tryEnqueue} throw with {@code reason}.
     *
     * @param reason why the enqueue was refused
     */
    record Reject(String reason) implements EnqueueDecision {}

    static EnqueueDecision allow() {
        return new Allow();
    }

    static EnqueueDecision skip() {
        return new Skip("");
    }

    static EnqueueDecision skip(String reason) {
        return new Skip(reason == null ? "" : reason);
    }

    static EnqueueDecision defer(Duration delay) {
        return new Defer(delay);
    }

    /** Defer until {@code instant}; resolves to a delay from now (zero if already past). */
    static EnqueueDecision deferUntil(Instant instant) {
        Duration delay = Duration.between(Instant.now(), instant);
        return new Defer(delay.isNegative() ? Duration.ZERO : delay);
    }

    static EnqueueDecision reject(String reason) {
        return new Reject(reason == null ? "" : reason);
    }
}
