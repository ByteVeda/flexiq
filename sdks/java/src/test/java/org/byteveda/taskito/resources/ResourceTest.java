package org.byteveda.taskito.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.ResourceException;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.task.TaskFunction;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class ResourceTest {

    private static final Task<Integer> TASK = Task.of("res.task", Integer.class);

    @Test
    @Timeout(30)
    void workerResourceSharedTaskResourcePerInvocation(@TempDir Path dir) throws Exception {
        int jobs = 3;
        AtomicInteger taskDisposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("r.db").toString()).open()) {
            queue.resource("shared", ctx -> new Object()); // WORKER scope (default): built once
            queue.resource("perTask", ResourceScope.TASK, ctx -> new Object(), value -> taskDisposed.incrementAndGet());

            CountDownLatch ran = new CountDownLatch(jobs);
            Worker worker = queue.worker()
                    .handle(TASK, p -> {
                        Resources.use("shared");
                        Resources.use("perTask");
                        ran.countDown();
                        return p;
                    })
                    .start();
            try (worker) {
                for (int i = 0; i < jobs; i++) {
                    queue.enqueue(TASK, i);
                }
                assertTrue(ran.await(20, TimeUnit.SECONDS), "handlers did not all run");
            } // worker close drains in-flight tasks, so every teardown has run

            Map<String, ResourceStat> metrics = queue.resourceMetrics();
            assertEquals(1, metrics.get("shared").created(), "worker resource built once");
            assertEquals(jobs, metrics.get("perTask").created(), "task resource built per invocation");
            assertEquals(jobs, metrics.get("perTask").disposed(), "task resource disposed per invocation");
            assertEquals(jobs, taskDisposed.get());
        }
    }

    @Test
    void useOutsideTaskThrows() {
        assertThrows(ResourceException.class, () -> Resources.use("anything"));
    }

    @Test
    void duplicateRegistrationThrows(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rdup.db").toString()).open()) {
            queue.resource("db", ctx -> new Object());
            assertThrows(ResourceException.class, () -> queue.resource("db", ctx -> new Object()));
        }
    }

    @Test
    @Timeout(30)
    void circularResourceDependencyIsRejected(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rcyc.db").toString()).open()) {
            queue.resource("self", ctx -> {
                ctx.use("self"); // self-reference — must fail fast, not StackOverflow
                return new Object();
            });
            AtomicReference<Class<?>> thrown = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(TASK, p -> {
                        try {
                            Resources.use("self");
                        } catch (RuntimeException e) {
                            thrown.set(e.getClass());
                        } finally {
                            ran.countDown();
                        }
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS));
            }
            assertEquals(ResourceException.class, thrown.get());
        }
    }

    @Test
    @Timeout(30)
    void threadResourceReusedAcrossSequentialTasks(@TempDir Path dir) throws Exception {
        int jobs = 2;
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rth.db").toString()).open()) {
            queue.resource("perThread", ResourceScope.THREAD, ctx -> new Object(), value -> disposed.incrementAndGet());
            AtomicReference<Object> first = new AtomicReference<>();
            AtomicReference<Object> second = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(jobs);
            Worker worker = queue.worker()
                    .concurrency(1) // one thread → both jobs share its instance
                    .handle(TASK, p -> {
                        Object instance = Resources.use("perThread");
                        if (!first.compareAndSet(null, instance)) {
                            second.set(instance);
                        }
                        ran.countDown();
                        return p;
                    })
                    .start();
            try (worker) {
                for (int i = 0; i < jobs; i++) {
                    queue.enqueue(TASK, i);
                }
                assertTrue(ran.await(20, TimeUnit.SECONDS));
            } // worker close disposes thread-scoped instances

            assertSame(first.get(), second.get(), "same thread reuses its instance");
            assertEquals(1, queue.resourceMetrics().get("perThread").created());
            assertEquals(1, disposed.get(), "disposed once at worker shutdown");
        }
    }

    @Test
    @Timeout(30)
    void requestResourceFreshPerUse(@TempDir Path dir) throws Exception {
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rrq.db").toString()).open()) {
            queue.resource("perUse", ResourceScope.REQUEST, ctx -> new Object(), value -> disposed.incrementAndGet());
            AtomicReference<Object> a = new AtomicReference<>();
            AtomicReference<Object> b = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(TASK, p -> {
                        a.set(Resources.use("perUse"));
                        b.set(Resources.use("perUse"));
                        ran.countDown();
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS));
            }
            assertNotSame(a.get(), b.get(), "every use builds a fresh instance");
            assertEquals(2, queue.resourceMetrics().get("perUse").created());
            assertEquals(2, disposed.get(), "both instances disposed at task end");
        }
    }

    @Test
    @Timeout(30)
    void threadFactoryCannotUseTaskScopedResource(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rtg.db").toString()).open()) {
            queue.resource("perTask", ResourceScope.TASK, ctx -> new Object());
            queue.resource("perThread", ResourceScope.THREAD, ctx -> ctx.use("perTask"));
            AtomicReference<Class<?>> thrown = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(TASK, p -> {
                        try {
                            Resources.use("perThread");
                        } catch (RuntimeException e) {
                            thrown.set(e.getClass());
                        } finally {
                            ran.countDown();
                        }
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS));
            }
            assertEquals(ResourceException.class, thrown.get());
        }
    }

    @Test
    @Timeout(30)
    void workerFactoryCannotUseThreadResource(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rwg.db").toString()).open()) {
            queue.resource("perThread", ResourceScope.THREAD, ctx -> new Object());
            queue.resource("shared", ctx -> ctx.use("perThread")); // WORKER factory
            AtomicReference<Class<?>> thrown = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(TASK, p -> {
                        try {
                            Resources.use("shared");
                        } catch (RuntimeException e) {
                            thrown.set(e.getClass());
                        } finally {
                            ran.countDown();
                        }
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS));
            }
            assertEquals(ResourceException.class, thrown.get());
        }
    }

    @Test
    @Timeout(30)
    void workerScopedResourceIsBuiltPerWorker(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rpw.db").toString()).open()) {
            queue.resource("perWorker", ctx -> new Object());
            // Each concurrency-1 worker holds its slot on `release` while running its job, so a
            // single worker cannot claim both jobs — the second stays for the other worker. We
            // wait until both instances are built rather than making the jobs rendezvous on a
            // barrier, which would fail if the two workers don't overlap within one tight window.
            CountDownLatch release = new CountDownLatch(1);
            TaskFunction<Integer, Integer> handler = p -> {
                Resources.use("perWorker");
                release.await(25, TimeUnit.SECONDS);
                return p;
            };
            Worker w1 = queue.worker().concurrency(1).handle(TASK, handler).start();
            try (w1) {
                // Nested rather than a second resource in the same header, so a failure to
                // start the second worker still closes the first.
                Worker w2 = queue.worker().concurrency(1).handle(TASK, handler).start();
                try (w2) {
                    queue.enqueue(TASK, 1);
                    queue.enqueue(TASK, 2);
                    assertTrue(
                            awaitCreated(queue, "perWorker", 2, Duration.ofSeconds(25)),
                            "both workers did not each build their own instance");
                    release.countDown();
                }
            }
            // Shared-runtime would build once (created==1); per-worker builds one each.
            assertEquals(2, queue.resourceMetrics().get("perWorker").created());
        }
    }

    @Test
    @Timeout(30)
    void pooledResourceReusedAcrossSequentialTasks(@TempDir Path dir) throws Exception {
        int jobs = 2;
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rpl.db").toString()).open()) {
            queue.resource("pooled", PoolConfig.of(1), ctx -> new Object(), value -> disposed.incrementAndGet());
            AtomicReference<Object> first = new AtomicReference<>();
            AtomicReference<Object> second = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(jobs);
            Worker worker = queue.worker()
                    .concurrency(1)
                    .handle(TASK, p -> {
                        Object instance = Resources.use("pooled");
                        if (!first.compareAndSet(null, instance)) {
                            second.set(instance);
                        }
                        ran.countDown();
                        return p;
                    })
                    .start();
            try (worker) {
                for (int i = 0; i < jobs; i++) {
                    queue.enqueue(TASK, i);
                }
                assertTrue(ran.await(20, TimeUnit.SECONDS));
            } // worker close shuts the pool down and disposes its instances

            assertSame(first.get(), second.get(), "a pool of one hands the same instance to sequential tasks");
            Map<String, ResourceStat> metrics = queue.resourceMetrics();
            assertEquals(1, metrics.get("pooled").created(), "one checkout at a time builds one instance");
            assertEquals(1, metrics.get("pooled").disposed(), "disposed at worker shutdown");
            assertEquals(1, disposed.get());
        }
    }

    @Test
    void pooledPoolMinCannotExceedPoolSize() {
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class, () -> PoolConfig.of(2).withPoolMin(3));
        assertTrue(thrown.getMessage().contains("poolMin must be <= poolSize"), thrown.getMessage());
    }

    @Test
    @Timeout(30)
    void pooledExhaustionTimesOut() {
        PoolConfig config = new PoolConfig(1, 0, Duration.ofMillis(50), null);
        ResourcePool pool = new ResourcePool("db", config, Object::new, null);
        assertNotNull(pool.acquire(), "first checkout takes the only permit");
        ResourceException thrown = assertThrows(ResourceException.class, pool::acquire);
        assertTrue(thrown.getMessage().contains("pool timed out"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("db"), thrown.getMessage());
        assertEquals(1, pool.stats().totalTimeouts());
    }

    @Test
    @Timeout(30)
    void pooledFactoryFailureDoesNotLeakCapacity() {
        int failures = 3;
        AtomicInteger attempts = new AtomicInteger();
        Supplier<Object> factory = () -> {
            if (attempts.incrementAndGet() <= failures) {
                throw new IllegalStateException("connect failed");
            }
            return new Object();
        };
        ResourcePool pool =
                new ResourcePool("flaky", new PoolConfig(1, 0, Duration.ofMillis(200), null), factory, null);
        for (int i = 0; i < failures; i++) {
            assertThrows(IllegalStateException.class, pool::acquire);
            assertEquals(0, pool.stats().active(), "a failed build must not count as checked out");
        }
        // The permit was returned on every failure, so a size-1 pool still has capacity.
        assertNotNull(pool.acquire());
        assertEquals(1, pool.stats().active());
    }

    @Test
    @Timeout(30)
    void pooledPrewarmBuildsMinInstances(@TempDir Path dir) throws Exception {
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rpp.db").toString()).open()) {
            queue.resource(
                    "warm", PoolConfig.of(4).withPoolMin(2), ctx -> new Object(), value -> disposed.incrementAndGet());
            Worker worker = queue.worker().handle(TASK, p -> p).start();
            try (worker) {
                assertEquals(
                        2,
                        queue.resourceMetrics().get("warm").created(),
                        "prewarm builds poolMin instances at worker start, before any job");
            }
            assertEquals(2, queue.resourceMetrics().get("warm").disposed());
            assertEquals(2, disposed.get());
        }
    }

    @Test
    @Timeout(30)
    void pooledMaxLifetimeDisposesExpired() throws Exception {
        AtomicInteger built = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        ResourcePool pool = new ResourcePool(
                "aging",
                new PoolConfig(1, 0, Duration.ofSeconds(5), Duration.ofMillis(20)),
                () -> "instance-" + built.incrementAndGet(),
                value -> disposed.incrementAndGet());
        Object firstInstance = pool.acquire();
        pool.release(firstInstance);
        Thread.sleep(60); // outlive maxLifetime while parked
        Object secondInstance = pool.acquire();
        assertNotSame(firstInstance, secondInstance, "an expired idle instance is replaced, not reused");
        assertEquals(2, built.get());
        assertEquals(1, disposed.get(), "the expired instance was disposed");
    }

    @Test
    @Timeout(30)
    void pooledFactoryCannotUseTaskScopedResource(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rpg.db").toString()).open()) {
            queue.resource("perTask", ResourceScope.TASK, ctx -> new Object());
            queue.resource("pooled", PoolConfig.of(1), ctx -> ctx.use("perTask"), value -> {});
            AtomicReference<RuntimeException> thrown = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(TASK, p -> {
                        try {
                            Resources.use("pooled");
                        } catch (RuntimeException e) {
                            thrown.set(e);
                        } finally {
                            ran.countDown();
                        }
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS));
            }
            assertEquals(ResourceException.class, thrown.get().getClass());
            assertTrue(
                    thrown.get().getMessage().contains("a pooled resource may only use worker resources"),
                    thrown.get().getMessage());
        }
    }

    @Test
    @Timeout(30)
    void pooledInstancesDisposedAtWorkerShutdown(@TempDir Path dir) throws Exception {
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rpd.db").toString()).open()) {
            queue.resource("conn", PoolConfig.of(2), ctx -> new Object(), value -> disposed.incrementAndGet());
            // Hold both tasks at a barrier so each checks its own instance out.
            CyclicBarrier bothBusy = new CyclicBarrier(2);
            CountDownLatch ran = new CountDownLatch(2);
            Worker worker = queue.worker()
                    .concurrency(2)
                    .handle(TASK, p -> {
                        Resources.use("conn");
                        bothBusy.await(20, TimeUnit.SECONDS);
                        ran.countDown();
                        return p;
                    })
                    .start();
            try (worker) {
                queue.enqueue(TASK, 1);
                queue.enqueue(TASK, 2);
                assertTrue(ran.await(25, TimeUnit.SECONDS));
            } // worker close returns both instances, then pool shutdown disposes them

            Map<String, ResourceStat> metrics = queue.resourceMetrics();
            assertEquals(2, metrics.get("conn").created(), "both concurrent tasks checked out an instance");
            assertEquals(2, metrics.get("conn").disposed(), "pool shutdown disposed every instance");
            assertEquals(2, disposed.get());
        }
    }

    /** Poll resource metrics until {@code name} has been built {@code target} times, or the deadline passes. */
    private static boolean awaitCreated(Taskito queue, String name, long target, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        do {
            ResourceStat stat = queue.resourceMetrics().get(name);
            if (stat != null && stat.created() >= target) {
                return true;
            }
            Thread.sleep(25);
        } while (System.nanoTime() < deadline);
        return false;
    }
}
