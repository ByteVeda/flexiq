package org.byteveda.flexiq.spi;

import org.jspecify.annotations.Nullable;

/**
 * Callbacks the worker runtime invokes (on native threads). The SDK implements
 * this to run tasks and surface outcomes.
 *
 * <p>{@code onJob} must return promptly — hand the work to an executor and
 * complete it later via {@link WorkerControl}. {@code onOutcome} reports a
 * finished job for events/middleware; its {@code wallTimeNs} is the execution
 * time the runtime measured, 0 when the run wasn't measured.
 */
public interface WorkerBridge {
    /**
     * Run one dispatched job.
     *
     * <p>{@code metadataJson} and {@code disabledMiddlewareJson} are the two
     * things an attached executor cannot look up for itself: it holds no
     * database credentials, so the scheduler resolves them and sends them with
     * the job. Both are {@code null} under an in-process worker, whose bridge
     * reads the live values from storage instead.
     *
     * @param metadataJson the job's stored metadata blob, or {@code null}
     * @param disabledMiddlewareJson a JSON array of disabled middleware names,
     *     or {@code null} when the caller should read the list itself
     */
    void onJob(
            long token,
            String jobId,
            String taskName,
            byte[] payload,
            @Nullable String metadataJson,
            @Nullable String disabledMiddlewareJson);

    /**
     * The form the runtime actually calls, carrying the dispatched attempt.
     *
     * <p>{@code attempt} is the job's {@code retryCount} at dispatch. Durable
     * steps are fenced on {@code (owner, attempt)}, and this is the attempt half
     * — the owner half stays on the worker handle, out of reach of anything an
     * attached executor could fill in from a socket frame.
     *
     * <p>A default rather than a signature change, so a bridge written against
     * the six-argument form keeps working; it simply cannot run durable steps.
     */
    default void onJob(
            long token,
            String jobId,
            String taskName,
            byte[] payload,
            @Nullable String metadataJson,
            @Nullable String disabledMiddlewareJson,
            int attempt) {
        onJob(token, jobId, taskName, payload, metadataJson, disabledMiddlewareJson);
    }

    void onOutcome(
            String kind,
            String jobId,
            String taskName,
            String error,
            int retryCount,
            boolean timedOut,
            long wallTimeNs);
}
