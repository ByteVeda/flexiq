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

    public JobInfo(String id, String taskName, Supplier<Map<String, Object>> metadataLoader) {
        this.id = id;
        this.taskName = taskName;
        this.metadataLoader = metadataLoader;
    }

    public String id() {
        return id;
    }

    public String taskName() {
        return taskName;
    }

    /** The job's metadata map (e.g. trace ids injected at enqueue); loaded on first call. */
    public Map<String, Object> metadata() {
        if (metadata == null) {
            metadata = metadataLoader.get();
        }
        return metadata;
    }
}
