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

    /** Resolve the named resource for the current task. */
    public static <T> @Nullable T use(String name) {
        TaskScope scope = ACTIVE.get();
        if (scope == null) {
            throw new ResourceException("Resources.use(\"" + name + "\") called outside a task handler");
        }
        return scope.use(name);
    }

    /** Bind {@code scope} for the current thread (called by the worker before the handler). */
    public static void enter(TaskScope scope) {
        ACTIVE.set(scope);
    }

    /**
     * Unbind the current thread's scope and dispose its task-scoped resources
     * (called by the worker after the handler, in a {@code finally}).
     */
    public static void exit(TaskScope scope) {
        ACTIVE.clear();
        if (scope != null) {
            scope.teardown();
        }
    }
}
