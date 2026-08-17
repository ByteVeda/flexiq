package org.byteveda.flexiq.task;

import java.time.Duration;
import java.util.Objects;

/**
 * Per-task circuit-breaker configuration. The breaker opens once {@link #threshold} failures
 * occur within {@link #window}, stays open for {@link #cooldown}, then admits up to
 * {@link #halfOpenProbes} probe runs and re-closes only if their success rate reaches
 * {@link #halfOpenSuccessRate}. Enforcement lives in the core scheduler; this only supplies the
 * configuration, registered when the worker starts.
 */
public final class CircuitBreakerConfig {
    private final int threshold;
    private final Duration window;
    private final Duration cooldown;
    private final int halfOpenProbes;
    private final double halfOpenSuccessRate;

    private CircuitBreakerConfig(Builder b) {
        this.threshold = b.threshold;
        this.window = b.window;
        this.cooldown = b.cooldown;
        this.halfOpenProbes = b.halfOpenProbes;
        this.halfOpenSuccessRate = b.halfOpenSuccessRate;
    }

    /** A builder for a breaker that opens after {@code threshold} failures in the window. */
    public static Builder builder(int threshold) {
        return new Builder(threshold);
    }

    /** A breaker with default window/cooldown/probe settings, opening after {@code threshold} failures. */
    public static CircuitBreakerConfig of(int threshold) {
        return builder(threshold).build();
    }

    public int threshold() {
        return threshold;
    }

    public Duration window() {
        return window;
    }

    public Duration cooldown() {
        return cooldown;
    }

    public int halfOpenProbes() {
        return halfOpenProbes;
    }

    public double halfOpenSuccessRate() {
        return halfOpenSuccessRate;
    }

    public static final class Builder {
        private final int threshold;
        private Duration window = Duration.ofSeconds(60);
        private Duration cooldown = Duration.ofSeconds(300);
        private int halfOpenProbes = 5;
        private double halfOpenSuccessRate = 0.8;

        private Builder(int threshold) {
            if (threshold <= 0) {
                throw new IllegalArgumentException("circuit-breaker threshold must be > 0");
            }
            this.threshold = threshold;
        }

        /** Rolling window in which failures are counted toward the threshold. */
        public Builder window(Duration window) {
            this.window = requirePositive(window, "window");
            return this;
        }

        /** Convenience for {@code window(Duration.ofSeconds(seconds))}. */
        public Builder windowSeconds(long seconds) {
            return window(Duration.ofSeconds(seconds));
        }

        /** How long the breaker stays open before admitting half-open probes. */
        public Builder cooldown(Duration cooldown) {
            this.cooldown = requirePositive(cooldown, "cooldown");
            return this;
        }

        /** Convenience for {@code cooldown(Duration.ofSeconds(seconds))}. */
        public Builder cooldownSeconds(long seconds) {
            return cooldown(Duration.ofSeconds(seconds));
        }

        /** Number of probe runs admitted while half-open. */
        public Builder halfOpenProbes(int halfOpenProbes) {
            if (halfOpenProbes <= 0) {
                throw new IllegalArgumentException("halfOpenProbes must be > 0");
            }
            this.halfOpenProbes = halfOpenProbes;
            return this;
        }

        /** Success rate (0.0–1.0) among probes required to re-close the breaker. */
        public Builder halfOpenSuccessRate(double halfOpenSuccessRate) {
            if (!(halfOpenSuccessRate >= 0.0 && halfOpenSuccessRate <= 1.0)) {
                throw new IllegalArgumentException("halfOpenSuccessRate must be within [0.0, 1.0]");
            }
            this.halfOpenSuccessRate = halfOpenSuccessRate;
            return this;
        }

        public CircuitBreakerConfig build() {
            return new CircuitBreakerConfig(this);
        }

        private static Duration requirePositive(Duration value, String name) {
            Objects.requireNonNull(value, name + " must not be null");
            if (value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
        }
    }
}
