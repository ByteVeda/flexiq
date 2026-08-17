package org.byteveda.flexiq.internal;

/** The default {@link NativeTransport}: the existing JNI entry points. */
final class JniTransport implements NativeTransport {
    private final long handle;

    JniTransport(long handle) {
        this.handle = handle;
    }

    @Override
    public String enqueue(String taskName, byte[] payload, String optionsJson) {
        return NativeQueue.enqueue(handle, taskName, payload, optionsJson);
    }

    @Override
    public String[] enqueueMany(String taskName, byte[][] payloads, String optionsJson) {
        return NativeQueue.enqueueMany(handle, taskName, payloads, optionsJson);
    }

    @Override
    public byte[] getResult(String jobId) {
        return NativeQueue.getResult(handle, jobId);
    }
}
