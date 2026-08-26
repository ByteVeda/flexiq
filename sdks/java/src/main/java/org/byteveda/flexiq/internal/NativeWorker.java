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

    public static native void completeJob(long handle, long token, byte[] result);

    public static native void failJob(long handle, long token, String error, boolean retryable);

    public static native void cancelJob(long handle, long token);

    /**
     * Report that an in-flight job's attempt ended in a durable step sleep.
     * {@code wakeAt} is the deadline the core rescheduled it to, in Unix
     * milliseconds.
     */
    public static native void sleepJob(long handle, long token, long wakeAt);

    /**
     * Open the durable-step session for one attempt of {@code jobId}; returns
     * its handle.
     *
     * <p>On the worker, because the {@code (owner, attempt)} fence's owner is
     * the id this worker won its execution claim under. Java supplies only the
     * job and the attempt.
     */
    public static native long openStepSession(long handle, String jobId, int attempt);

    public static native void stop(long handle);

    public static native void close(long handle);

    /** A JSON {@code ClusterInfo} snapshot, or {@code null} when not mesh-enabled. */
    public static native String meshClusterInfo(long handle);
}
