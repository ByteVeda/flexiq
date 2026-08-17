package org.byteveda.flexiq.resources;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Per-invocation resource scope. Caches task-scoped resources for one task and
 * disposes them (LIFO) when the task ends. Also serves as the
 * {@link ResourceContext} for a task-scoped factory, which may use worker or task
 * resources. Not thread-safe — confined to the single thread running the task.
 */
public final class TaskScope implements ResourceContext {
    private final ResourceRuntime runtime;
    private final Map<String, Object> cache = new HashMap<>();
    private final Deque<Runnable> teardown = new ArrayDeque<>();

    TaskScope(ResourceRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public ResourceScope scope() {
        return ResourceScope.TASK;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> @Nullable T use(String name) {
        return (T) runtime.resolveForTask(this, name);
    }

    Map<String, Object> cache() {
        return cache;
    }

    void pushTeardown(Runnable disposer) {
        teardown.push(disposer);
    }

    /** Dispose this invocation's task-scoped resources in reverse build order. */
    void teardown() {
        while (!teardown.isEmpty()) {
            teardown.pop().run();
        }
    }
}
