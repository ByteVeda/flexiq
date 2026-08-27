package org.byteveda.flexiq.middleware;

import java.util.Map;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * The executing job, exposed to {@link Middleware} hooks. {@link #metadata()} is
 * loaded lazily (a storage read) only when first accessed, so middleware that
 * never reads it pays nothing.
 */
public final class JobInfo {
    private final String id;
    private final String taskName;
    private final Supplier<Map<String, Object>> metadataLoader;
    private @Nullable Map<String, Object> metadata;

    /**
     * A job view whose metadata is a storage read waiting to happen.
     *
     * @param id the job's id
     * @param taskName the task's registered name
     * @param metadataLoader reads the metadata blob; called at most once, and only
     *     if a hook asks for it
     */
    public JobInfo(String id, String taskName, Supplier<Map<String, Object>> metadataLoader) {
        this.id = id;
        this.taskName = taskName;
        this.metadataLoader = metadataLoader;
    }

    /**
     * The job's id.
     *
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * The task's registered name.
     *
     * @return the name
     */
    public String taskName() {
        return taskName;
    }

    /**
     * The job's metadata map (e.g. trace ids injected at enqueue); loaded on first call.
     *
     * @return the metadata, read once and cached for the rest of the execution
     */
    public Map<String, Object> metadata() {
        if (metadata == null) {
            metadata = metadataLoader.get();
        }
        return metadata;
    }
}
