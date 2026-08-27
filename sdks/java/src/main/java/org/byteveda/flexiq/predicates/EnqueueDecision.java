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
        /** Refuses a negative delay, which would ask for an enqueue in the past. */
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

    /**
     * Proceed with the enqueue unchanged.
     *
     * @return an {@link Allow}
     */
    static EnqueueDecision allow() {
        return new Allow();
    }

    /**
     * Drop the enqueue without a reason.
     *
     * @return a {@link Skip} with an empty reason
     */
    static EnqueueDecision skip() {
        return new Skip("");
    }

    /**
     * Drop the enqueue, saying why.
     *
     * @param reason why it was skipped; {@code null} becomes an empty reason
     * @return a {@link Skip}
     */
    static EnqueueDecision skip(String reason) {
        return new Skip(reason == null ? "" : reason);
    }

    /**
     * Enqueue, held back by {@code delay}.
     *
     * @param delay how long to hold the job back; must not be negative
     * @return a {@link Defer}
     */
    static EnqueueDecision defer(Duration delay) {
        return new Defer(delay);
    }

    /**
     * Defer until {@code instant}; resolves to a delay from now (zero if already past).
     *
     * @param instant when the job should become runnable
     * @return a {@link Defer} carrying the delay to that instant
     */
    static EnqueueDecision deferUntil(Instant instant) {
        Duration delay = Duration.between(Instant.now(), instant);
        return new Defer(delay.isNegative() ? Duration.ZERO : delay);
    }

    /**
     * Refuse the enqueue, so both {@code enqueue} and {@code tryEnqueue} throw.
     *
     * @param reason why it was refused; {@code null} becomes an empty reason
     * @return a {@link Reject}
     */
    static EnqueueDecision reject(String reason) {
        return new Reject(reason == null ? "" : reason);
    }
}
