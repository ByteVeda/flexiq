package org.byteveda.flexiq.spi;

import java.util.Optional;
import org.byteveda.flexiq.steps.StepUnavailableError;
import org.jspecify.annotations.Nullable;

/** Controls a running worker and completes its in-flight jobs. */
public interface WorkerControl extends AutoCloseable {
    void completeJob(long token, byte[] result);

    /** Fail a job. {@code retryable} false dead-letters it whatever budget is left. */
    void failJob(long token, String error, boolean retryable);

    void cancelJob(long token);

    /**
     * Report that an attempt ended in a durable {@code step.sleep}.
     *
     * <p>Neither a completion nor a failure: the sleep row is already committed,
     * the execution claim released and the job {@code Pending} at
     * {@code wakeAt}, so this only tells the scheduler what happened. No retry
     * is spent, no budget token, no circuit-breaker transition, no metric.
     *
     * <p>Unreachable unless {@link #openStepSession} handed out a session, so
     * the default refuses rather than guessing.
     *
     * @param wakeAt the deadline the job was rescheduled to, in Unix milliseconds
     */
    default void sleepJob(long token, long wakeAt) {
        throw new StepUnavailableError("this worker cannot report a durable step sleep");
    }

    /**
     * Open the durable-step session for one attempt of {@code jobId}.
     *
     * <p>On the <b>worker control</b>, not the queue, because every step write is
     * fenced on {@code (owner, attempt)} and the owner must be the id this
     * worker won its execution claim under — never something the running task
     * asserts about itself. A queue-level slot would be overwritten by a second
     * worker on the same handle, and every step the first worker went on to
     * commit would then be refused as superseded.
     *
     * <p>The default refuses, which is the honest answer for an attached
     * executor (no storage, and no channel to commit a step on) and for any
     * backend without a step store. Retryable: a heterogeneous fleet
     * mid-rollout may put the next attempt somewhere that can commit.
     *
     * @param attempt the {@code retryCount} the job was dispatched with, checked
     *     against the row so a superseded attempt cannot write into the live one
     */
    default StepSession openStepSession(String jobId, int attempt) {
        throw new StepUnavailableError("durable steps need a worker that reaches storage; this one has none, so job "
                + jobId + " cannot commit a step");
    }

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
