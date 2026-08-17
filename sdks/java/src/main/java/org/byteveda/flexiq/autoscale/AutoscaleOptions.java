package org.byteveda.flexiq.autoscale;

import java.time.Duration;

/**
 * Tuning for the {@link Autoscaler}.
 *
 * @param minWorkers the floor on handler threads (must be &ge; 1)
 * @param maxWorkers the ceiling on handler threads (must be &ge; {@code minWorkers})
 * @param tasksPerWorker desired outstanding tasks per worker thread (must be &ge; 1)
 * @param interval how often to re-evaluate (must be positive)
 */
public record AutoscaleOptions(int minWorkers, int maxWorkers, int tasksPerWorker, Duration interval) {
    public AutoscaleOptions {
        if (minWorkers < 1) {
            throw new IllegalArgumentException("minWorkers must be >= 1");
        }
        if (maxWorkers < minWorkers) {
            throw new IllegalArgumentException("maxWorkers must be >= minWorkers");
        }
        if (tasksPerWorker < 1) {
            throw new IllegalArgumentException("tasksPerWorker must be >= 1");
        }
        if (interval == null || interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }

    /** Defaults: scale {@code min..max} threads, ~10 tasks per worker, re-evaluated every 2s. */
    public static AutoscaleOptions of(int minWorkers, int maxWorkers) {
        return new AutoscaleOptions(minWorkers, maxWorkers, 10, Duration.ofSeconds(2));
    }
}
