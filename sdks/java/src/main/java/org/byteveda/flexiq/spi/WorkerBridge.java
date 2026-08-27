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
     * @param token the dispatch token to complete or fail the job with
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param payload the encoded payload, to decode with the queue's serializer
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
     *
     * @param token the dispatch token to complete or fail the job with
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param payload the encoded payload, to decode with the queue's serializer
     * @param metadataJson the job's stored metadata blob, or {@code null}
     * @param disabledMiddlewareJson a JSON array of disabled middleware names,
     *     or {@code null} when the caller should read the list itself
     * @param attempt the job's {@code retryCount} at dispatch, the attempt half of the
     *     step fence
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

    /**
     * Report a finished job, for events and middleware.
     *
     * @param kind the core's verdict: {@code success}, {@code retry}, {@code dead} or
     *     {@code cancelled}
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param error the stored error string, or {@code null} on success and cancel
     * @param retryCount attempts spent, or {@code -1} where it does not apply
     * @param timedOut whether the attempt was cut short by the task timeout
     * @param wallTimeNs how long the run took, or 0 when it was not measured
     */
    void onOutcome(
            String kind,
            String jobId,
            String taskName,
            String error,
            int retryCount,
            boolean timedOut,
            long wallTimeNs);
}
