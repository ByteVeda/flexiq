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

    public static native void stop(long handle);

    public static native void close(long handle);

    /** A JSON {@code ClusterInfo} snapshot, or {@code null} when not mesh-enabled. */
    public static native String meshClusterInfo(long handle);
}
