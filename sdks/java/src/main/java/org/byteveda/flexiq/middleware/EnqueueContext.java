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
    /** The task the caller asked for; a redirect is an interceptor's job, not a hook's. */
    public final String taskName;

    private @Nullable Object payload;
    private EnqueueOptions options;
    private final Map<String, Object> metadata = new HashMap<>();

    /**
     * The enqueue as the caller framed it, before any hook has run.
     *
     * @param taskName the task the caller asked for
     * @param payload what the caller passed, before serialization
     * @param options the delivery settings the caller passed
     */
    public EnqueueContext(String taskName, @Nullable Object payload, EnqueueOptions options) {
        this.taskName = taskName;
        this.payload = payload;
        this.options = options;
    }

    /**
     * Mutable metadata that travels with the job (readable at execution via
     * {@code TaskContext.job().metadata()}). When non-empty it becomes the job's
     * metadata blob, replacing any set on the options.
     *
     * @return the live map — write into it to attach metadata
     */
    public Map<String, Object> metadata() {
        return metadata;
    }

    /**
     * What will be serialized, as it stands after the hooks that ran already.
     *
     * @return the payload, or {@code null} for a task that takes none
     */
    public @Nullable Object payload() {
        return payload;
    }

    /**
     * Rewrite what gets serialized.
     *
     * @param payload the replacement; later hooks and the serializer see this
     */
    public void payload(@Nullable Object payload) {
        this.payload = payload;
    }

    /**
     * The delivery settings, as they stand after the hooks that ran already.
     *
     * @return the options
     */
    public EnqueueOptions options() {
        return options;
    }

    /**
     * Rewrite the delivery settings.
     *
     * @param options the replacement, wholesale; later hooks and the enqueue see this
     */
    public void options(EnqueueOptions options) {
        this.options = options;
    }
}
