package org.byteveda.flexiq.internal;

/**
 * The hot byte-transfer ops ({@code enqueue}, {@code enqueueMany},
 * {@code getResult}) behind a swappable transport. The default transport is JNI
 * ({@link JniTransport}); on a JDK that supports Project Panama (FFM) the faster
 * {@code FfmTransport} overlay is selected instead. Everything else on
 * {@link NativeQueue} stays on JNI — this seam covers only the per-job hot path.
 */
public interface NativeTransport {

    /**
     * Enqueue one job.
     *
     * @param taskName the task's registered name
     * @param payload the encoded payload, opaque to the core
     * @param optionsJson the options as JSON
     * @return the new job's id
     */
    String enqueue(String taskName, byte[] payload, String optionsJson);

    /**
     * Enqueue a batch in one call.
     *
     * @param taskName the task's registered name
     * @param payloads one encoded payload per job
     * @param optionsJson a JSON array of per-job options, the same length as {@code payloads}
     * @return the job ids, in input order
     */
    String[] enqueueMany(String taskName, byte[][] payloads, String optionsJson);

    /**
     * A completed job's stored result.
     *
     * @param jobId the job's id
     * @return the encoded result, or {@code null} if absent or incomplete
     */
    byte[] getResult(String jobId);

    /**
     * The best transport for {@code handle}: the FFM fast path when its overlay
     * class resolves (JDK 22+ via the multi-release jar) and its native symbols
     * link, otherwise JNI. Any failure to initialize FFM falls back to JNI, so
     * the seam never breaks the 17 floor.
     *
     * @param handle the queue handle from {@link NativeQueue#open}
     * @return the FFM transport where it links, otherwise the JNI one
     */
    static NativeTransport create(long handle) {
        try {
            Class<?> ffm = Class.forName("org.byteveda.flexiq.internal.FfmTransport");
            return (NativeTransport) ffm.getMethod("create", long.class).invoke(null, handle);
        } catch (ReflectiveOperationException | LinkageError ffmUnavailable) {
            return new JniTransport(handle);
        }
    }
}
