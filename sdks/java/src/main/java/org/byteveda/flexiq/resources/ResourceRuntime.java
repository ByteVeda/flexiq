package org.byteveda.flexiq.resources;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.byteveda.flexiq.errors.ResourceException;
import org.jspecify.annotations.Nullable;

/**
 * Registry and lifecycle for worker resources. The client-level instance holds
 * the definitions and per-resource counters; each worker gets its own live
 * runtime via {@link #forWorker()}, sharing those definitions/counters but with
 * its own cache of worker-scoped instances — so a {@code WORKER}-scoped resource
 * is one instance <em>per worker</em>, not one per client. Worker-scoped
 * resources are built at most once per worker (under a per-name lock, so a
 * concurrent first-use does not double-build) and disposed LIFO when that
 * worker's last lease is released.
 */
public final class ResourceRuntime {
    private static final Logger LOG = System.getLogger(ResourceRuntime.class.getName());
    /** Cache sentinel for a factory that legitimately returned {@code null}. */
    private static final Object NULL = new Object();

    private final ConcurrentMap<String, ResourceDefinition> definitions;
    private final ConcurrentMap<String, Counter> counters;
    private final ConcurrentMap<String, Object> workerCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReentrantLock> workerLocks = new ConcurrentHashMap<>();
    /**
     * Per-name, per-thread instances for {@code THREAD}-scoped resources. Only the
     * owning thread reads or writes its own entry (so no per-name lock is needed);
     * the concurrent maps exist for cross-thread visibility at worker teardown.
     */
    private final ConcurrentMap<String, ConcurrentMap<Thread, Object>> threadCache = new ConcurrentHashMap<>();
    /**
     * Per-name pools for {@code POOLED}-scoped resources. An instance field like
     * {@code workerCache}, so each worker runtime from {@link #forWorker()} owns
     * its own pools — capacity is per worker, never shared across workers.
     */
    private final ConcurrentMap<String, ResourcePool> pools = new ConcurrentHashMap<>();
    /**
     * Worker resources each worker resource used, recorded at build time so
     * {@link #reload} can rebuild a dependency before the resources that used it.
     */
    private final ConcurrentMap<String, Set<String>> workerDeps = new ConcurrentHashMap<>();

    private final Deque<Teardown> workerTeardown = new ArrayDeque<>();
    /** Names being resolved on the current thread, so a dependency cycle fails fast instead of recursing. */
    private final ThreadLocal<Set<String>> resolving = ThreadLocal.withInitial(LinkedHashSet::new);

    /** The client runtime this one was forked from, or {@code null} on a client runtime itself. */
    private final @Nullable ResourceRuntime parent;
    /** Leased per-worker runtimes, so a client-level {@link #reload} reaches the live caches. */
    private final Set<ResourceRuntime> workerRuntimes = ConcurrentHashMap.newKeySet();

    private int leases; // guarded by this
    /** Set once this runtime's last lease dropped and its instances were swept; guarded by this. */
    private boolean disposed; // guarded by this

    /** One instance's disposal, tagged with its resource name so a reload can retire just that resource. */
    private record Teardown(String name, Runnable action) {}

    /** Context handed to a thread factory: it may use worker or thread resources. */
    private final ResourceContext threadContext = new ResourceContext() {
        @Override
        public ResourceScope scope() {
            return ResourceScope.THREAD;
        }

        @Override
        public <T> @Nullable T use(String name) {
            return cast(resolveThread(name));
        }
    };

    /**
     * Context handed to a pooled factory: it may only use worker resources —
     * pooled instances outlive tasks, so a task/request/thread-scoped dependency
     * would dangle after its own scope ends.
     */
    private final ResourceContext pooledContext = new ResourceContext() {
        @Override
        public ResourceScope scope() {
            return ResourceScope.POOLED;
        }

        @Override
        public <T> @Nullable T use(String name) {
            return cast(resolvePooledDependency(name));
        }
    };

    /** A client-level runtime: holds definitions + counters, hands each worker a child via {@link #forWorker()}. */
    public ResourceRuntime() {
        this.definitions = new ConcurrentHashMap<>();
        this.counters = new ConcurrentHashMap<>();
        this.parent = null;
    }

    private ResourceRuntime(
            ConcurrentMap<String, ResourceDefinition> definitions,
            ConcurrentMap<String, Counter> counters,
            @Nullable ResourceRuntime parent) {
        this.definitions = definitions;
        this.counters = counters;
        this.parent = parent;
    }

    /**
     * A per-worker runtime sharing this runtime's definitions and counters but with
     * its own worker-scoped cache, teardown stack, and lease count — so each worker
     * builds and disposes its own {@code WORKER}-scoped instances.
     */
    public ResourceRuntime forWorker() {
        return new ResourceRuntime(definitions, counters, this);
    }

    /** Register a resource under {@code name}. A name may be registered only once. */
    public void register(String name, ResourceDefinition definition) {
        if (definitions.putIfAbsent(name, definition) != null) {
            throw new ResourceException("resource '" + name + "' is already registered");
        }
        counters.computeIfAbsent(name, key -> new Counter());
    }

    /** Whether any resource is registered (lets the worker skip all wiring when unused). */
    public boolean isEmpty() {
        return definitions.isEmpty();
    }

    /** A fresh per-invocation scope for one task. */
    public TaskScope createTaskScope() {
        return new TaskScope(this);
    }

    /** Lease the worker resources (paired with {@link #teardownWorker}); the first lease prewarms pools. */
    public void acquireWorker() {
        boolean firstLease;
        synchronized (this) {
            firstLease = leases == 0;
            leases++;
        }
        // Outside the monitor: prewarm runs user factories, which may be slow —
        // holding the lock would stall every concurrent lease/teardown. A racing
        // shutdown is safe: the pool disposes prewarmed instances once closed.
        if (firstLease) {
            // Publish to the client runtime only once live, so a builder that is
            // never started can't leave a reload target behind.
            if (parent != null) {
                parent.workerRuntimes.add(this);
            }
            prewarmPools();
        }
    }

    /** Release a worker lease; when the last one drops, dispose worker resources LIFO. */
    public synchronized void teardownWorker() {
        if (leases > 0) {
            leases--;
        }
        if (leases == 0) {
            disposeWorker();
        }
    }

    /**
     * Hot-reload resources: dispose what is cached and rebuild, so the next use sees
     * a fresh instance. Returns {@code name -> success}; an unregistered name reports
     * {@code false} rather than throwing.
     *
     * <p>{@code names} reloads exactly those, whatever their {@code reloadable} flag
     * says; a {@code null} {@code names} sweeps every definition registered with
     * {@code reloadable}. Targets are ordered dependency-first, so a dependent
     * resolves the fresh dependency rather than the retired one.
     *
     * <p>On a client-level runtime the instances live in the per-worker runtimes, so
     * this fans out to every live worker and reports a name as reloaded only when it
     * reloaded everywhere. The result is empty when no worker is running.
     *
     * @param names the resources to reload, or {@code null} for the reloadable sweep
     * @return per-resource reload success
     */
    public Map<String, Boolean> reload(@Nullable Collection<String> names) {
        if (parent != null) {
            return reloadHere(names);
        }
        Map<String, Boolean> merged = new LinkedHashMap<>();
        for (ResourceRuntime runtime : workerRuntimes) {
            runtime.reloadHere(names).forEach((name, ok) -> merged.merge(name, ok, Boolean::logicalAnd));
        }
        return merged;
    }

    /**
     * Reload this runtime's own instances, dependencies before their dependents.
     *
     * <p>Runs under the runtime monitor — the one {@link #teardownWorker} takes — so a
     * worker closing concurrently waits the reload out instead of sweeping the caches
     * from under it and stranding a freshly built instance whose disposer would then
     * never run. A runtime already torn down reloads nothing.
     */
    private synchronized Map<String, Boolean> reloadHere(@Nullable Collection<String> names) {
        if (disposed) {
            return Map.of();
        }
        Set<String> targets = new LinkedHashSet<>();
        if (names != null) {
            targets.addAll(names);
        } else {
            definitions.forEach((name, definition) -> {
                if (definition.reloadable()) {
                    targets.add(name);
                }
            });
        }
        Map<String, Boolean> results = new LinkedHashMap<>();
        for (String name : dependencyOrder(targets)) {
            if (targets.contains(name)) {
                results.put(name, reloadOne(name));
            }
        }
        return results;
    }

    /**
     * {@code targets} plus the resources they were built from, ordered so a resource
     * follows everything it used. Only the targets are reloaded; the extra names are
     * there to place them.
     */
    private List<String> dependencyOrder(Set<String> targets) {
        List<String> ordered = new ArrayList<>();
        Set<String> done = new LinkedHashSet<>();
        for (String name : targets) {
            visitDependencies(name, new LinkedHashSet<>(), done, ordered);
        }
        return ordered;
    }

    private void visitDependencies(String name, Set<String> chain, Set<String> done, List<String> ordered) {
        // `chain` breaks dependency cycles; a self-referencing factory is legal.
        if (done.contains(name) || !chain.add(name)) {
            return;
        }
        for (String dependency : workerDeps.getOrDefault(name, Set.of())) {
            visitDependencies(dependency, chain, done, ordered);
        }
        chain.remove(name);
        done.add(name);
        ordered.add(name);
    }

    /** Reload one resource according to its scope. An unknown name reports false. */
    private boolean reloadOne(String name) {
        ResourceDefinition definition = definitions.get(name);
        if (definition == null) {
            return false;
        }
        switch (definition.scope()) {
            case WORKER:
                return recreateWorker(name);
            case THREAD:
                // Each worker thread rebuilds its own instance on next use; there is
                // no thread to build them on from here.
                retireCached(name);
                return true;
            case POOLED:
                // Drop the pool so the next checkout builds a fresh one; idle instances
                // are disposed now and checked-out ones when they are released.
                ResourcePool pool = pools.remove(name);
                if (pool != null) {
                    pool.shutdown();
                }
                return true;
            default:
                // Task- and request-scoped resources are built per invocation —
                // nothing is cached to replace, so a reload is a successful no-op.
                return true;
        }
    }

    /**
     * Dispose the cached worker instance and build a fresh one eagerly, so a factory
     * that now fails is reported rather than surfacing on some later task. Holds the
     * resource's build lock so a concurrent first resolve cannot interleave.
     */
    private boolean recreateWorker(String name) {
        ReentrantLock lock = workerLocks.computeIfAbsent(name, key -> new ReentrantLock());
        lock.lock();
        try {
            retireCached(name);
            resolveWorker(name);
            return true;
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "reloading resource '" + name + "' failed", e);
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** Dispose and forget every cached instance of {@code name}, worker- and thread-scoped. */
    private void retireCached(String name) {
        workerCache.remove(name);
        workerDeps.remove(name);
        threadCache.remove(name);
        List<Teardown> retired = new ArrayList<>();
        synchronized (workerTeardown) {
            // ArrayDeque iterates head→tail and push() adds at the head, so the
            // collected entries stay in LIFO order.
            Iterator<Teardown> entries = workerTeardown.iterator();
            while (entries.hasNext()) {
                Teardown entry = entries.next();
                if (entry.name().equals(name)) {
                    retired.add(entry);
                    entries.remove();
                }
            }
        }
        // Outside the monitor: a user disposer may be slow, and holding it would
        // stall every concurrent build's teardown registration.
        for (Teardown entry : retired) {
            entry.action().run();
        }
    }

    /** Per-resource counters snapshot. */
    public Map<String, ResourceStat> metrics() {
        Map<String, ResourceStat> out = new LinkedHashMap<>();
        counters.forEach((name, counter) -> {
            long created = counter.created.get();
            long disposed = counter.disposed.get();
            out.put(name, new ResourceStat(created, disposed, created - disposed));
        });
        return out;
    }

    /** Resolve a worker-scoped resource, building it once under a per-name lock. */
    @Nullable
    Object resolveWorker(String name) {
        ResourceDefinition definition = definition(name);
        if (definition.scope() != ResourceScope.WORKER) {
            throw new ResourceException("resource '" + name + "' is "
                    + scopeWord(definition.scope())
                    + "-scoped; a worker resource may only use worker resources");
        }
        Object cached = workerCache.get(name);
        if (cached != null) {
            return unwrap(cached);
        }
        ReentrantLock lock = workerLocks.computeIfAbsent(name, key -> new ReentrantLock());
        lock.lock();
        try {
            cached = workerCache.get(name);
            if (cached != null) {
                return unwrap(cached);
            }
            Object value = build(name, definition, workerContext(name));
            workerCache.put(name, value == null ? NULL : value);
            counter(name).created.incrementAndGet();
            Consumer<Object> disposer = definition.dispose();
            if (disposer != null) {
                pushTeardown(name, () -> dispose(name, value, disposer));
            }
            return value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Context handed to a worker factory: it may only use other worker resources.
     * Each build gets its own, recording the dependencies it resolves — rebuilt from
     * scratch every time so a changed factory can't leave a dependency it no longer
     * uses in the reload ordering.
     */
    private ResourceContext workerContext(String name) {
        Set<String> deps = ConcurrentHashMap.newKeySet();
        workerDeps.put(name, deps);
        return new ResourceContext() {
            @Override
            public ResourceScope scope() {
                return ResourceScope.WORKER;
            }

            @Override
            public <T> @Nullable T use(String dependency) {
                deps.add(dependency);
                return cast(resolveWorker(dependency));
            }
        };
    }

    /** Queue one instance's disposal on the shared LIFO teardown stack. */
    private void pushTeardown(String name, Runnable action) {
        synchronized (workerTeardown) {
            workerTeardown.push(new Teardown(name, action));
        }
    }

    /**
     * Resolve for a task, dispatching by the resource's scope: worker hits the
     * shared cache, thread hits the current thread's cache, request builds fresh
     * on every use, pooled checks an instance out for the invocation, task builds
     * once per invocation.
     */
    @Nullable
    Object resolveForTask(TaskScope scope, String name) {
        ResourceDefinition definition = definition(name);
        switch (definition.scope()) {
            case WORKER:
                return resolveWorker(name);
            case THREAD:
                return resolveThread(name);
            case REQUEST:
                return buildRequest(scope, name, definition);
            case POOLED:
                return resolvePooled(scope, name, definition);
            case TASK:
            default:
                break;
        }
        Map<String, Object> cache = scope.cache();
        Object cached = cache.get(name);
        if (cached != null) {
            return unwrap(cached);
        }
        Object value = build(name, definition, scope);
        cache.put(name, value == null ? NULL : value);
        counter(name).created.incrementAndGet();
        Consumer<Object> disposer = definition.dispose();
        if (disposer != null) {
            scope.pushTeardown(() -> dispose(name, value, disposer));
        }
        return value;
    }

    /**
     * Resolve a thread-scoped resource for the current worker thread, building it
     * lazily. Only the owning thread touches its own map entry, so a plain
     * get-build-put is race-free; the shared {@code workerTeardown} deque keeps
     * disposal globally LIFO across worker and thread instances.
     */
    @Nullable
    Object resolveThread(String name) {
        ResourceDefinition definition = definition(name);
        if (definition.scope() == ResourceScope.WORKER) {
            return resolveWorker(name);
        }
        if (definition.scope() != ResourceScope.THREAD) {
            throw new ResourceException("resource '" + name + "' is "
                    + scopeWord(definition.scope())
                    + "-scoped; a thread resource may only use worker or thread resources");
        }
        ConcurrentMap<Thread, Object> perThread = threadCache.computeIfAbsent(name, key -> new ConcurrentHashMap<>());
        Object cached = perThread.get(Thread.currentThread());
        if (cached != null) {
            return unwrap(cached);
        }
        Object value = build(name, definition, threadContext);
        perThread.put(Thread.currentThread(), value == null ? NULL : value);
        counter(name).created.incrementAndGet();
        Consumer<Object> disposer = definition.dispose();
        if (disposer != null) {
            pushTeardown(name, () -> dispose(name, value, disposer));
        }
        return value;
    }

    /** Build a request-scoped resource: fresh on every use, disposed with the task (LIFO). */
    private Object buildRequest(TaskScope scope, String name, ResourceDefinition definition) {
        Object value = build(name, definition, requestContext(scope));
        counter(name).created.incrementAndGet();
        Consumer<Object> disposer = definition.dispose();
        if (disposer != null) {
            scope.pushTeardown(() -> dispose(name, value, disposer));
        }
        return value;
    }

    /**
     * Resolve a pooled resource: one checkout per task per resource, cached in the
     * task scope like a task-scoped instance and returned to the pool (not
     * disposed) when the task ends.
     */
    private @Nullable Object resolvePooled(TaskScope scope, String name, ResourceDefinition definition) {
        Map<String, Object> cache = scope.cache();
        Object cached = cache.get(name);
        if (cached != null) {
            return unwrap(cached);
        }
        ResourcePool pool = pool(name, definition);
        Object value = pool.acquire();
        cache.put(name, value == null ? NULL : value);
        scope.pushTeardown(() -> pool.release(value));
        return value;
    }

    /** Resolve a pooled factory's dependency, enforcing the worker-only guard. */
    private @Nullable Object resolvePooledDependency(String name) {
        ResourceDefinition definition = definition(name);
        if (definition.scope() != ResourceScope.WORKER) {
            throw new ResourceException("resource '" + name + "' is "
                    + scopeWord(definition.scope())
                    + "-scoped; a pooled resource may only use worker resources");
        }
        return resolveWorker(name);
    }

    /** The pool for {@code name}, created lazily on first use. */
    private ResourcePool pool(String name, ResourceDefinition definition) {
        return pools.computeIfAbsent(name, key -> createPool(name, definition));
    }

    /**
     * A pool whose factory and disposer keep the per-resource counters honest:
     * {@code created} moves only when the factory builds, {@code disposed} only
     * when the pool actually disposes — checkout/return never touch them.
     */
    private ResourcePool createPool(String name, ResourceDefinition definition) {
        return new ResourcePool(
                name,
                definition.requirePool(),
                () -> {
                    Object value = build(name, definition, pooledContext);
                    counter(name).created.incrementAndGet();
                    return value;
                },
                value -> disposePooled(name, value, definition.dispose()));
    }

    /** Dispose one pooled instance; without a disposer the drop still counts as disposed. */
    private void disposePooled(String name, Object value, @Nullable Consumer<Object> disposer) {
        if (disposer == null) {
            counter(name).disposed.incrementAndGet();
            return;
        }
        dispose(name, value, disposer);
    }

    /** Eagerly build {@code poolMin} instances for every pooled resource that asks for prewarm. */
    private void prewarmPools() {
        definitions.forEach((name, definition) -> {
            if (definition.scope() == ResourceScope.POOLED
                    && definition.requirePool().poolMin() > 0) {
                pool(name, definition).prewarm();
            }
        });
    }

    /** Context handed to a request factory: dependencies resolve through the active task scope. */
    private ResourceContext requestContext(TaskScope scope) {
        return new ResourceContext() {
            @Override
            public ResourceScope scope() {
                return ResourceScope.REQUEST;
            }

            @Override
            public <T> @Nullable T use(String name) {
                return scope.use(name);
            }
        };
    }

    private Object build(String name, ResourceDefinition definition, ResourceContext context) {
        Set<String> chain = resolving.get();
        if (!chain.add(name)) {
            throw new ResourceException("circular resource dependency at '" + name + "' (chain: " + chain + ")");
        }
        try {
            return definition.factory().apply(context);
        } catch (ResourceException e) {
            throw e; // a nested unknown/cycle/build failure already carries its message
        } catch (RuntimeException e) {
            throw new ResourceException("failed to build resource '" + name + "'", e);
        } finally {
            chain.remove(name);
        }
    }

    private void disposeWorker() {
        // Set under the monitor reloadHere() also holds, so a reload either finishes
        // before this sweep or sees the runtime as gone — it can never cache an
        // instance (and queue its disposer) behind the sweep that already ran.
        disposed = true;
        if (parent != null) {
            parent.workerRuntimes.remove(this);
        }
        // Pools first: pooled instances may depend on worker resources, which the
        // teardown stack below disposes.
        for (ResourcePool pool : pools.values()) {
            pool.shutdown();
        }
        pools.clear();
        synchronized (workerTeardown) {
            while (!workerTeardown.isEmpty()) {
                workerTeardown.pop().action().run();
            }
        }
        workerCache.clear();
        workerDeps.clear();
        threadCache.clear();
    }

    private static String scopeWord(ResourceScope scope) {
        return scope.name().toLowerCase(Locale.ROOT);
    }

    private void dispose(String name, Object value, Consumer<Object> disposer) {
        try {
            disposer.accept(unwrap(value));
            counter(name).disposed.incrementAndGet();
        } catch (RuntimeException e) {
            // A disposer must never fail teardown — record and continue.
            LOG.log(Level.WARNING, "disposing resource '" + name + "' failed", e);
        }
    }

    private ResourceDefinition definition(String name) {
        ResourceDefinition definition = definitions.get(name);
        if (definition == null) {
            throw new ResourceException("unknown resource '" + name + "'");
        }
        return definition;
    }

    private Counter counter(String name) {
        return counters.computeIfAbsent(name, key -> new Counter());
    }

    private static @Nullable Object unwrap(Object value) {
        return value == NULL ? null : value;
    }

    @SuppressWarnings("unchecked")
    private static <T> @Nullable T cast(@Nullable Object value) {
        return (T) value;
    }

    /** Mutable per-resource counters. */
    private static final class Counter {
        final AtomicLong created = new AtomicLong();
        final AtomicLong disposed = new AtomicLong();
    }
}
