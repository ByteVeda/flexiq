package org.byteveda.flexiq.spi;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

/** Controls a running worker and completes its in-flight jobs. */
public interface WorkerControl extends AutoCloseable {
    void completeJob(long token, byte[] result);

    /** Fail a job. {@code retryable} false dead-letters it whatever budget is left. */
    void failJob(long token, String error, boolean retryable);

    void cancelJob(long token);

    /**
     * Record a running job's progress (0-100).
     *
     * <p>Implemented only by an attached executor, which has no storage of its
     * own: the scheduler holds the connection and applies this on its behalf.
     * A worker writes straight to its {@link QueueBackend} and never calls this,
     * so the default does nothing.
     */
    default void reportProgress(String jobId, int progress) {}

    /**
     * Write one structured log line for a running job. A published partial is
     * this at level {@code "result"}, with the value as {@code extra}.
     *
     * <p>Same split as {@link #reportProgress}.
     */
    default void writeTaskLog(String jobId, String taskName, String level, String message, @Nullable String extra) {}

    /** Stop the scheduler and heartbeat loops; in-flight jobs drain. */
    void stop();

    /** A JSON {@code ClusterInfo} snapshot when mesh is enabled, else empty. */
    default Optional<String> meshClusterInfoJson() {
        return Optional.empty();
    }

    @Override
    void close();
}
