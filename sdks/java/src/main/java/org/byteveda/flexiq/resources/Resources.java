package org.byteveda.flexiq.resources;

import org.byteveda.flexiq.errors.ResourceException;
import org.byteveda.flexiq.internal.ScopeContext;
import org.jspecify.annotations.Nullable;

/**
 * Resolve worker resources from inside a task handler: {@code Resources.use("db")}.
 * Valid only while a task runs on this worker; the worker binds the task's scope
 * around the handler call.
 */
public final class Resources {
    private static final ScopeContext<TaskScope> ACTIVE = new ScopeContext<>();

    private Resources() {}

    /**
     * Resolve the named resource for the current task.
     *
     * @param name the resource's registered name
     * @param <T> what the caller expects it to be; unchecked, so a mismatch surfaces
     *     as a {@link ClassCastException} at the use site
     * @return the resolved resource, or {@code null} if its factory produced none
     */
    public static <T> @Nullable T use(String name) {
        TaskScope scope = ACTIVE.get();
        if (scope == null) {
            throw new ResourceException("Resources.use(\"" + name + "\") called outside a task handler");
        }
        return scope.use(name);
    }

    /**
     * Bind {@code scope} for the current thread (called by the worker before the handler).
     *
     * @param scope the invocation's scope, which {@link #use} resolves against
     */
    public static void enter(TaskScope scope) {
        ACTIVE.set(scope);
    }

    /**
     * Unbind the current thread's scope and dispose its task-scoped resources
     * (called by the worker after the handler, in a {@code finally}).
     *
     * @param scope the invocation's scope, torn down here; {@code null} is tolerated
     *     so the {@code finally} needs no guard of its own
     */
    public static void exit(TaskScope scope) {
        ACTIVE.clear();
        if (scope != null) {
            scope.teardown();
        }
    }
}
