package org.byteveda.flexiq.resources;

import org.jspecify.annotations.Nullable;

/**
 * Handed to a resource factory so it can depend on other resources. A factory
 * may only depend on same-or-longer-lived resources: {@code WORKER} and
 * {@code POOLED} factories may {@link #use} only worker resources (a pooled
 * instance outlives the task that built it), a {@code THREAD} factory may use
 * worker or thread resources, and {@code TASK}/{@code REQUEST} factories may
 * use any scope.
 */
public interface ResourceContext {
    /** The scope the factory is building for. */
    ResourceScope scope();

    /** Resolve another resource by name (building it if needed). */
    <T> @Nullable T use(String name);
}
