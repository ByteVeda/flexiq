package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A task's circuit-breaker state as observed by the core. {@code state} is the lowercase wire
 * value ({@code "closed"}, {@code "open"}, or {@code "half_open"}). Timestamps are Unix
 * milliseconds, or {@code null} when the breaker has not reached that transition.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class CircuitBreakerState {
    /** The task this breaker guards. */
    public final String taskName;

    /** The lowercase wire state: {@code closed}, {@code open} or {@code half_open}. */
    public final String state;

    /** Failures counted inside the current window. */
    public final int failureCount;

    /** How many failures in the window trip the breaker. */
    public final int threshold;

    /** The rolling window failures are counted over. */
    public final long windowMs;

    /** How long the breaker stays open before admitting probes. */
    public final long cooldownMs;

    /** When it last tripped, in Unix milliseconds, or {@code null} if it never has. */
    public final Long openedAt;

    /** When the last counted failure landed, in Unix milliseconds, or {@code null}. */
    public final Long lastFailureAt;

    /** Probe runs admitted while half-open. */
    public final int halfOpenMaxProbes;

    /** The share of probes that must succeed to re-close it. */
    public final double halfOpenSuccessRate;

    /**
     * Decoded from the core's JSON breaker view.
     *
     * @param taskName the task this breaker guards
     * @param state the lowercase wire state: {@code closed}, {@code open} or {@code half_open}
     * @param failureCount failures counted inside the current window
     * @param threshold how many failures in the window trip the breaker
     * @param windowMs the rolling window failures are counted over
     * @param cooldownMs how long the breaker stays open before admitting probes
     * @param openedAt when it last tripped, in Unix milliseconds, or {@code null} if it never has
     * @param lastFailureAt when the last counted failure landed, in Unix milliseconds, or {@code null}
     * @param halfOpenMaxProbes probe runs admitted while half-open
     * @param halfOpenSuccessRate the share of probes that must succeed to re-close it
     */
    @JsonCreator
    public CircuitBreakerState(
            @JsonProperty("taskName") String taskName,
            @JsonProperty("state") String state,
            @JsonProperty("failureCount") int failureCount,
            @JsonProperty("threshold") int threshold,
            @JsonProperty("windowMs") long windowMs,
            @JsonProperty("cooldownMs") long cooldownMs,
            @JsonProperty("openedAt") Long openedAt,
            @JsonProperty("lastFailureAt") Long lastFailureAt,
            @JsonProperty("halfOpenMaxProbes") int halfOpenMaxProbes,
            @JsonProperty("halfOpenSuccessRate") double halfOpenSuccessRate) {
        this.taskName = taskName;
        this.state = state;
        this.failureCount = failureCount;
        this.threshold = threshold;
        this.windowMs = windowMs;
        this.cooldownMs = cooldownMs;
        this.openedAt = openedAt;
        this.lastFailureAt = lastFailureAt;
        this.halfOpenMaxProbes = halfOpenMaxProbes;
        this.halfOpenSuccessRate = halfOpenSuccessRate;
    }

    /**
     * Whether the breaker is currently open (fast-failing this task).
     *
     * @return whether {@link #state} is {@code open}
     */
    public boolean isOpen() {
        return "open".equals(state);
    }
}
