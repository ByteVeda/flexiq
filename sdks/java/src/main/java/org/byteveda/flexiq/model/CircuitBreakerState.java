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
    public final String taskName;
    public final String state;
    public final int failureCount;
    public final int threshold;
    public final long windowMs;
    public final long cooldownMs;
    public final Long openedAt;
    public final Long lastFailureAt;
    public final int halfOpenMaxProbes;
    public final double halfOpenSuccessRate;

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

    /** Whether the breaker is currently open (fast-failing this task). */
    public boolean isOpen() {
        return "open".equals(state);
    }
}
