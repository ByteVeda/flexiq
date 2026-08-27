package org.byteveda.flexiq.task;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A task's retry-backoff curve: how long to wait between attempts.
 *
 * <p>The retry <em>budget</em> (how many attempts) is set per enqueue via
 * {@link Task#maxRetries}. The core scheduler owns retry execution, so retries
 * stay durable, survive worker crashes, and behave identically to the other
 * FlexiQ SDKs — this type only supplies the timing the scheduler applies.
 *
 * <p>Instances are immutable.
 */
public final class RetryPolicy {
    private final @Nullable Duration baseDelay;
    private final @Nullable Duration maxDelay;
    private final List<Duration> customDelays;

    private RetryPolicy(@Nullable Duration baseDelay, @Nullable Duration maxDelay, List<Duration> customDelays) {
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.customDelays = customDelays;
    }

    /**
     * Exponential backoff: retry N waits about {@code base · 2^N}, capped at
     * {@code max}, plus a random jitter of up to {@code base} (spreads retries so
     * a batch of failures doesn't retry in lockstep).
     *
     * @param base the first wait, and the jitter's span; must not be negative
     * @param max the cap the curve flattens at; must not be negative
     * @return the policy
     */
    public static RetryPolicy exponential(Duration base, Duration max) {
        return new RetryPolicy(nonNegative(base, "base"), nonNegative(max, "max"), Collections.emptyList());
    }

    /**
     * Explicit per-attempt delays, applied exactly (no jitter): retry N waits
     * {@code delays[N]}. The list is authoritative for the retries it covers, so
     * supply at least as many delays as the task's {@code maxRetries} — once the
     * list is exhausted any further retries fire immediately.
     *
     * @param delays one wait per retry, in order; at least one, none negative
     * @return the policy
     */
    public static RetryPolicy delays(Duration... delays) {
        if (delays.length == 0) {
            throw new IllegalArgumentException("at least one delay is required");
        }
        List<Duration> copy = new ArrayList<>(delays.length);
        for (Duration delay : delays) {
            copy.add(nonNegative(delay, "delay"));
        }
        return new RetryPolicy(null, null, Collections.unmodifiableList(copy));
    }

    /**
     * The exponential base delay.
     *
     * @return the base, or {@code null} when the policy came from {@link #delays}
     */
    public @Nullable Duration baseDelay() {
        return baseDelay;
    }

    /**
     * The backoff cap.
     *
     * @return the cap, or {@code null} when the policy came from {@link #delays}
     */
    public @Nullable Duration maxDelay() {
        return maxDelay;
    }

    /**
     * The explicit per-attempt delays.
     *
     * @return the delays, or an empty list when the policy is exponential
     */
    public List<Duration> customDelays() {
        return customDelays;
    }

    private static Duration nonNegative(Duration value, String what) {
        Objects.requireNonNull(value, what + " must not be null");
        if (value.isNegative()) {
            throw new IllegalArgumentException(what + " must not be negative");
        }
        return value;
    }
}
