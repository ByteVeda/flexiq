package org.byteveda.flexiq.middleware;

import java.util.HashMap;
import java.util.Map;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.jspecify.annotations.Nullable;

/**
 * A job being enqueued, passed to {@link Middleware#onEnqueue} before
 * serialization. Replace the payload or options to rewrite the job, add to
 * {@link #metadata()} to travel with the job, or throw to abort the enqueue.
 */
public final class EnqueueContext {
    public final String taskName;
    private @Nullable Object payload;
    private EnqueueOptions options;
    private final Map<String, Object> metadata = new HashMap<>();

    public EnqueueContext(String taskName, @Nullable Object payload, EnqueueOptions options) {
        this.taskName = taskName;
        this.payload = payload;
        this.options = options;
    }

    /**
     * Mutable metadata that travels with the job (readable at execution via
     * {@code TaskContext.job().metadata()}). When non-empty it becomes the job's
     * metadata blob, replacing any set on the options.
     */
    public Map<String, Object> metadata() {
        return metadata;
    }

    public @Nullable Object payload() {
        return payload;
    }

    public void payload(@Nullable Object payload) {
        this.payload = payload;
    }

    public EnqueueOptions options() {
        return options;
    }

    public void options(EnqueueOptions options) {
        this.options = options;
    }
}
