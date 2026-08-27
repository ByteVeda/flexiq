package org.byteveda.flexiq.resources;

import java.time.Duration;
import org.jspecify.annotations.Nullable;

/**
 * Sizing and timing for a {@link ResourceScope#POOLED} resource (part of the
 * cross-SDK contract for pooled resources).
 *
 * @param poolSize the maximum number of concurrently checked-out instances (must be &gt; 0)
 * @param poolMin how many instances to build eagerly at worker start (defaults to 0, at most
 *     {@code poolSize})
 * @param acquireTimeout how long a checkout may wait for capacity (defaults to 10s)
 * @param maxLifetime an idle instance older than this is disposed instead of reused
 *     ({@code null} keeps instances forever)
 */
public record PoolConfig(int poolSize, int poolMin, Duration acquireTimeout, @Nullable Duration maxLifetime) {

    private static final Duration DEFAULT_ACQUIRE_TIMEOUT = Duration.ofSeconds(10);

    /** Defaults the acquire timeout and refuses a sizing that could never hand out an instance. */
    public PoolConfig {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be > 0");
        }
        if (poolMin < 0) {
            throw new IllegalArgumentException("poolMin must be >= 0");
        }
        if (poolMin > poolSize) {
            throw new IllegalArgumentException("poolMin must be <= poolSize");
        }
        if (acquireTimeout == null) {
            acquireTimeout = DEFAULT_ACQUIRE_TIMEOUT;
        }
        if (acquireTimeout.isNegative() || acquireTimeout.isZero()) {
            throw new IllegalArgumentException("acquireTimeout must be positive");
        }
        if (maxLifetime != null && (maxLifetime.isNegative() || maxLifetime.isZero())) {
            throw new IllegalArgumentException("maxLifetime must be positive");
        }
    }

    /**
     * A pool of {@code poolSize} with no prewarm, a 10s acquire timeout, and unlimited lifetime.
     *
     * @param poolSize the maximum number of concurrently checked-out instances
     * @return the sizing
     */
    public static PoolConfig of(int poolSize) {
        return new PoolConfig(poolSize, 0, DEFAULT_ACQUIRE_TIMEOUT, null);
    }

    /**
     * This configuration with {@code poolMin} instances built eagerly at worker start.
     *
     * @param poolMin how many to prewarm; at most {@link #poolSize()}
     * @return the copy
     */
    public PoolConfig withPoolMin(int poolMin) {
        return new PoolConfig(poolSize, poolMin, acquireTimeout, maxLifetime);
    }

    /**
     * This configuration with a different checkout wait limit.
     *
     * @param acquireTimeout how long a checkout may wait for capacity; must be positive
     * @return the copy
     */
    public PoolConfig withAcquireTimeout(Duration acquireTimeout) {
        return new PoolConfig(poolSize, poolMin, acquireTimeout, maxLifetime);
    }

    /**
     * This configuration with a maximum instance lifetime ({@code null} for unlimited).
     *
     * @param maxLifetime an idle instance older than this is disposed instead of reused
     * @return the copy
     */
    public PoolConfig withMaxLifetime(Duration maxLifetime) {
        return new PoolConfig(poolSize, poolMin, acquireTimeout, maxLifetime);
    }
}
