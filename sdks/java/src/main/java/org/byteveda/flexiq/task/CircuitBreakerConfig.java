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

    /**
     * A builder for a breaker that opens after {@code threshold} failures in the window.
     *
     * @param threshold how many failures in the window trip it; must be positive
     * @return the builder, pre-loaded with the default window, cooldown and probes
     */
    public static Builder builder(int threshold) {
        return new Builder(threshold);
    }

    /**
     * A breaker with default window/cooldown/probe settings, opening after {@code threshold} failures.
     *
     * @param threshold how many failures in the window trip it; must be positive
     * @return the configuration
     */
    public static CircuitBreakerConfig of(int threshold) {
        return builder(threshold).build();
    }

    /**
     * How many failures in the window trip the breaker.
     *
     * @return the threshold
     */
    public int threshold() {
        return threshold;
    }

    /**
     * The rolling window failures are counted over.
     *
     * @return the window
     */
    public Duration window() {
        return window;
    }

    /**
     * How long the breaker stays open before admitting probes.
     *
     * @return the cooldown
     */
    public Duration cooldown() {
        return cooldown;
    }

    /**
     * Probe runs admitted while half-open.
     *
     * @return the probe count
     */
    public int halfOpenProbes() {
        return halfOpenProbes;
    }

    /**
     * The share of probes that must succeed to re-close the breaker.
     *
     * @return the rate, from 0.0 to 1.0
     */
    public double halfOpenSuccessRate() {
        return halfOpenSuccessRate;
    }

    /** Collects a breaker's optional settings around a required threshold. */
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

        /**
         * Rolling window in which failures are counted toward the threshold.
         *
         * @param window the window; must be positive
         * @return {@code this}, for chaining
         */
        public Builder window(Duration window) {
            this.window = requirePositive(window, "window");
            return this;
        }

        /**
         * Convenience for {@code window(Duration.ofSeconds(seconds))}.
         *
         * @param seconds the window in seconds; must be positive
         * @return {@code this}, for chaining
         */
        public Builder windowSeconds(long seconds) {
            return window(Duration.ofSeconds(seconds));
        }

        /**
         * How long the breaker stays open before admitting half-open probes.
         *
         * @param cooldown the cooldown; must be positive
         * @return {@code this}, for chaining
         */
        public Builder cooldown(Duration cooldown) {
            this.cooldown = requirePositive(cooldown, "cooldown");
            return this;
        }

        /**
         * Convenience for {@code cooldown(Duration.ofSeconds(seconds))}.
         *
         * @param seconds the cooldown in seconds; must be positive
         * @return {@code this}, for chaining
         */
        public Builder cooldownSeconds(long seconds) {
            return cooldown(Duration.ofSeconds(seconds));
        }

        /**
         * Number of probe runs admitted while half-open.
         *
         * @param halfOpenProbes the probe count; must be positive
         * @return {@code this}, for chaining
         */
        public Builder halfOpenProbes(int halfOpenProbes) {
            if (halfOpenProbes <= 0) {
                throw new IllegalArgumentException("halfOpenProbes must be > 0");
            }
            this.halfOpenProbes = halfOpenProbes;
            return this;
        }

        /**
         * Success rate (0.0–1.0) among probes required to re-close the breaker.
         *
         * @param halfOpenSuccessRate the rate, within {@code [0.0, 1.0]}
         * @return {@code this}, for chaining
         */
        public Builder halfOpenSuccessRate(double halfOpenSuccessRate) {
            if (!(halfOpenSuccessRate >= 0.0 && halfOpenSuccessRate <= 1.0)) {
                throw new IllegalArgumentException("halfOpenSuccessRate must be within [0.0, 1.0]");
            }
            this.halfOpenSuccessRate = halfOpenSuccessRate;
            return this;
        }

        /**
         * Freeze the settings collected so far.
         *
         * @return the immutable configuration
         */
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
