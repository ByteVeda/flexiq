package org.byteveda.flexiq.internal;

/**
 * JNI completion + control surface for a running worker.
 *
 * <p>The {@code handle} is the worker pointer from {@link NativeQueue#runWorker}.
 * {@code completeJob}/{@code failJob}/{@code cancelJob} resolve an in-flight job
 * identified by its {@code token} (delivered to {@code WorkerBridge.onJob}).
 */
public final class NativeWorker {
    static {
        NativeLoader.load();
    }

    private NativeWorker() {}

    /**
     * Report that an in-flight job finished successfully.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     * @param token the dispatch token delivered to {@code WorkerBridge.onJob}
     * @param result the encoded result to store
     */
    public static native void completeJob(long handle, long token, byte[] result);

    /**
     * Report that an in-flight job failed.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     * @param token the dispatch token delivered to {@code WorkerBridge.onJob}
     * @param error the stored error string
     * @param retryable {@code false} to dead-letter now, whatever budget is left
     */
    public static native void failJob(long handle, long token, String error, boolean retryable);

    /**
     * Report that an in-flight job was cancelled.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     * @param token the dispatch token delivered to {@code WorkerBridge.onJob}
     */
    public static native void cancelJob(long handle, long token);

    /**
     * Report that an in-flight job's attempt ended in a durable step sleep.
     * {@code wakeAt} is the deadline the core rescheduled it to, in Unix
     * milliseconds.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     * @param token the dispatch token delivered to {@code WorkerBridge.onJob}
     * @param wakeAt the deadline the core rescheduled the job to, in Unix milliseconds
     */
    public static native void sleepJob(long handle, long token, long wakeAt);

    /**
     * Open the durable-step session for one attempt of {@code jobId}; returns
     * its handle.
     *
     * <p>On the worker, because the {@code (owner, attempt)} fence's owner is
     * the id this worker won its execution claim under. Java supplies only the
     * job and the attempt.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     * @param jobId the running job
     * @param attempt the {@code retryCount} the job was dispatched with
     * @return the session handle
     */
    public static native long openStepSession(long handle, String jobId, int attempt);

    /**
     * Stop the scheduler and heartbeat loops; in-flight jobs drain.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     */
    public static native void stop(long handle);

    /**
     * Drain and reclaim the handle.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     */
    public static native void close(long handle);

    /**
     * A JSON {@code ClusterInfo} snapshot, or {@code null} when not mesh-enabled.
     *
     * @param handle the worker handle from {@link NativeQueue#runWorker}
     * @return the snapshot as JSON, or {@code null}
     */
    public static native String meshClusterInfo(long handle);
}
