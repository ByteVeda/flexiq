package org.byteveda.taskito.internal;

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

    /** Dial, handshake, and start running jobs. {@code bridge} is a {@code WorkerBridge}. */
    public static native long attach(Object bridge, String optionsJson);

    public static native void completeJob(long handle, long token, byte[] result);

    public static native void failJob(long handle, long token, String error, boolean retryable);

    public static native void cancelJob(long handle, long token);

    /** Identity the scheduler announced when it accepted the attach. */
    public static native String schedulerId(long handle);

    /** Identity this executor attached under. */
    public static native String executorId(long handle);

    /** Peer label of the scheduler connection. */
    public static native String peer(long handle);

    /** Whether the scheduler session is still open. */
    public static native boolean isRunning(long handle);

    /** Block until the scheduler ends the session. Parks the calling thread. */
    public static native void awaitSession(long handle);

    /** Stop accepting work and finish what is in flight. Returns at once. */
    public static native void stop(long handle);

    /** Drain, disconnect, and reclaim the handle. */
    public static native void close(long handle);
}
