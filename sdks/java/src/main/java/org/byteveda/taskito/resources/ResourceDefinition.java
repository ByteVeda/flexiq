package org.byteveda.taskito.resources;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * How to build (and optionally dispose) a resource.
 *
 * @param factory builds the resource, possibly using others via the context
 * @param scope the resource's lifetime (defaults to {@link ResourceScope#WORKER})
 * @param dispose optional cleanup run when the scope ends ({@code null} for none)
 * @param pool bounded-pool sizing, required for {@link ResourceScope#POOLED} and
 *     {@code null} for every other scope
 * @param reloadable whether a no-argument {@code Taskito.reloadResources()} sweep
 *     includes this resource; naming it explicitly reloads it either way
 */
public record ResourceDefinition(
        Function<ResourceContext, Object> factory,
        ResourceScope scope,
        @Nullable Consumer<Object> dispose,
        @Nullable PoolConfig pool,
        boolean reloadable) {

    public ResourceDefinition {
        if (factory == null) {
            throw new IllegalArgumentException("resource factory must not be null");
        }
        if (scope == null) {
            scope = ResourceScope.WORKER;
        }
        if (scope == ResourceScope.POOLED && pool == null) {
            throw new IllegalArgumentException("a pooled resource requires a PoolConfig");
        }
        if (scope != ResourceScope.POOLED && pool != null) {
            throw new IllegalArgumentException("a PoolConfig is only valid for a pooled resource");
        }
    }

    /** A definition that a no-argument reload sweep skips. */
    public ResourceDefinition(
            Function<ResourceContext, Object> factory,
            ResourceScope scope,
            @Nullable Consumer<Object> dispose,
            @Nullable PoolConfig pool) {
        this(factory, scope, dispose, pool, false);
    }

    /** A definition for any non-pooled scope. */
    public ResourceDefinition(
            Function<ResourceContext, Object> factory, ResourceScope scope, @Nullable Consumer<Object> dispose) {
        this(factory, scope, dispose, null);
    }

    /** The pool sizing of a {@link ResourceScope#POOLED} definition, which always has one. */
    public PoolConfig requirePool() {
        return Objects.requireNonNull(pool, "a pooled resource requires a PoolConfig");
    }

    /** A copy included in a no-argument {@code Taskito.reloadResources()} sweep. */
    public ResourceDefinition withReloadable(boolean reloadable) {
        return new ResourceDefinition(factory, scope, dispose, pool, reloadable);
    }

    /** A worker-scoped resource with no disposer. */
    public static ResourceDefinition worker(Function<ResourceContext, Object> factory) {
        return new ResourceDefinition(factory, ResourceScope.WORKER, null);
    }

    /** A task-scoped resource with no disposer. */
    public static ResourceDefinition task(Function<ResourceContext, Object> factory) {
        return new ResourceDefinition(factory, ResourceScope.TASK, null);
    }
}
