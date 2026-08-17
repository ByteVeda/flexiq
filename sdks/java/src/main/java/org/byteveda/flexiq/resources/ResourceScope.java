package org.byteveda.flexiq.resources;

/**
 * Lifetime of a worker resource. Names and wire forms are shared across SDKs; two are
 * platform-bound — {@link #THREAD} has no Node equivalent (one event loop, so no per-thread
 * identity) and {@link #REQUEST} has no Python equivalent (resources are injected once per task).
 */
public enum ResourceScope {
    /** Built once, lazily, and shared across every task on the worker. */
    WORKER,
    /**
     * Built lazily once per worker thread and shared by every task that runs on
     * that thread; disposed when the worker shuts down. If the pool retires a
     * thread early (cached pools, autoscale), its instance is retained until
     * worker teardown — bounded by the number of threads the pool ever created.
     */
    THREAD,
    /** Built lazily per task invocation and disposed when the task ends. */
    TASK,
    /**
     * Built fresh on every {@code use()} and disposed when the task ends —
     * never cached, so N uses inside one task yield N instances.
     */
    REQUEST,
    /**
     * A bounded pool of instances shared across tasks: each task checks out one
     * instance for its duration and returns it at task end. Capacity is bounded
     * by the {@link PoolConfig} supplied at registration; instances are disposed
     * at worker shutdown or when their {@link PoolConfig#maxLifetime()} expires.
     */
    POOLED
}
