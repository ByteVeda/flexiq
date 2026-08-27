package org.byteveda.flexiq.scaler;

import org.jspecify.annotations.Nullable;

/**
 * Configures the {@link Scaler} HTTP endpoint.
 *
 * @param port the port to bind ({@code 0} picks an ephemeral port)
 * @param host the bind address (defaults to {@code 0.0.0.0})
 * @param targetQueueDepth the depth the autoscaler targets per replica (must be &gt; 0)
 * @param queue the queue to report on, or {@code null} for all queues
 */
public record ScalerOptions(int port, String host, int targetQueueDepth, @Nullable String queue) {
    /** Defaults an absent host and refuses a target depth that would divide by zero. */
    public ScalerOptions {
        if (targetQueueDepth <= 0) {
            throw new IllegalArgumentException("targetQueueDepth must be > 0");
        }
        if (host == null || host.isBlank()) {
            host = "0.0.0.0";
        }
    }

    /**
     * Defaults: port 9090, all queues, target depth 10.
     *
     * @return the default options
     */
    public static ScalerOptions defaults() {
        return new ScalerOptions(9090, "0.0.0.0", 10, null);
    }

    /**
     * Bind to {@code port} with the other defaults.
     *
     * @param port the port to bind ({@code 0} picks an ephemeral one)
     * @return the options
     */
    public static ScalerOptions onPort(int port) {
        return new ScalerOptions(port, "0.0.0.0", 10, null);
    }
}
