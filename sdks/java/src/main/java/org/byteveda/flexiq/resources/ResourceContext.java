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
    /**
     * The scope the factory is building for.
     *
     * @return the scope, which bounds what {@link #use} will resolve
     */
    ResourceScope scope();

    /**
     * Resolve another resource by name (building it if needed).
     *
     * @param name the dependency's registered name
     * @param <T> what the caller expects it to be; unchecked, so a mismatch surfaces
     *     as a {@link ClassCastException} at the use site
     * @return the resolved resource, or {@code null} if its factory produced none
     */
    <T> @Nullable T use(String name);
}
