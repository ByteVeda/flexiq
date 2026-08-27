package org.byteveda.flexiq.internal;

import org.jspecify.annotations.Nullable;

/**
 * JNI surface for an executor attached to a detached scheduler.
 *
 * <p>The mirror of {@link NativeWorker}: the same token-based completion, over a
 * socket to a scheduler rather than over this process's own storage. The
 * {@code handle} comes from {@link #attach}; it stays valid until {@link #close}.
 */
public final class NativeExecutor {
    static {
        NativeLoader.load();
    }

    private NativeExecutor() {}

    /**
     * Dial, handshake, and start running jobs. {@code bridge} is a {@code WorkerBridge}.
     *
     * @param bridge the {@code WorkerBridge} each dispatched job is handed to
     * @param optionsJson the executor options as JSON
     * @return the executor handle
     */
    public static native long attach(Object bridge, String optionsJson);

    /**
     * Report that a dispatched job finished successfully.
     *
     * @param handle the executor handle from {@link #attach}
     * @param token the dispatch token the bridge was handed
     * @param result the encoded result to store
     */
    public static native void completeJob(long handle, long token, byte[] result);

    /**
     * Report that a dispatched job failed.
     *
     * @param handle the executor handle from {@link #attach}
     * @param token the dispatch token the bridge was handed
     * @param error the stored error string
     * @param retryable {@code false} to dead-letter now, whatever budget is left
     */
    public static native void failJob(long handle, long token, String error, boolean retryable);

    /**
     * Report that a dispatched job was cancelled.
     *
     * @param handle the executor handle from {@link #attach}
     * @param token the dispatch token the bridge was handed
     */
    public static native void cancelJob(long handle, long token);

    /**
     * Report a running job's progress (0-100) to the scheduler.
     *
     * <p>An executor holds no database credentials, so the scheduler applies
     * this on its behalf. Fire-and-forget: it neither blocks nor fails.
     *
     * @param handle the executor handle from {@link #attach}
     * @param jobId the job's id
     * @param progress the percentage the handler is reporting
     */
    public static native void reportProgress(long handle, String jobId, int progress);

    /**
     * Write one structured log line; {@code extra} may be null.
     *
     * @param handle the executor handle from {@link #attach}
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param level the severity's wire form
     * @param message the line itself
     * @param extra structured context as JSON, or {@code null}
     */
    public static native void writeTaskLog(
            long handle, String jobId, String taskName, String level, String message, @Nullable String extra);

    /**
     * Identity the scheduler announced when it accepted the attach.
     *
     * @param handle the executor handle from {@link #attach}
     * @return the scheduler's id
     */
    public static native String schedulerId(long handle);

    /**
     * Identity this executor attached under.
     *
     * @param handle the executor handle from {@link #attach}
     * @return the id the scheduler knows this executor by
     */
    public static native String executorId(long handle);

    /**
     * Peer label of the scheduler connection.
     *
     * @param handle the executor handle from {@link #attach}
     * @return the label, for logs and diagnostics
     */
    public static native String peer(long handle);

    /**
     * Whether the scheduler session is still open.
     *
     * @param handle the executor handle from {@link #attach}
     * @return whether the session is still open
     */
    public static native boolean isRunning(long handle);

    /**
     * Block until the scheduler ends the session. Parks the calling thread.
     *
     * @param handle the executor handle from {@link #attach}
     */
    public static native void awaitSession(long handle);

    /**
     * Stop accepting work and finish what is in flight. Returns at once.
     *
     * @param handle the executor handle from {@link #attach}
     */
    public static native void stop(long handle);

    /**
     * Drain, disconnect, and reclaim the handle.
     *
     * @param handle the executor handle from {@link #attach}
     */
    public static native void close(long handle);
}
