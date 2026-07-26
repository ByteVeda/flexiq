package org.byteveda.taskito.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class ResourceReloadTest {

    private static final Task<Integer> TASK = Task.of("reload.task", Integer.class);

    /** A worker-scoped definition that stamps each build and counts disposals. */
    private static ResourceDefinition counted(
            String prefix, AtomicInteger built, AtomicInteger disposed, boolean reloadable) {
        Function<ResourceContext, Object> factory = ctx -> prefix + "-" + built.incrementAndGet();
        return new ResourceDefinition(
                factory, ResourceScope.WORKER, value -> disposed.incrementAndGet(), null, reloadable);
    }

    @Test
    @Timeout(30)
    void reloadRebuildsAWorkerResource(@TempDir Path dir) throws Exception {
        AtomicInteger built = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        AtomicReference<String> seen = new AtomicReference<>();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rl.db").toString()).open()) {
            queue.resource("db", counted("conn", built, disposed, true));

            CountDownLatch ran = new CountDownLatch(1);
            try (Worker worker = queue.worker()
                    .handle(TASK, payload -> {
                        seen.set(Resources.use("db"));
                        ran.countDown();
                        return payload;
                    })
                    .start()) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS), "handler did not run");
                assertEquals("conn-1", seen.get());

                assertEquals(Map.of("db", true), queue.reloadResources());
                assertEquals(1, disposed.get(), "the retired instance is disposed");
                // Rebuilt eagerly, so a factory that now fails is reported here.
                assertEquals(2, built.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void noArgumentSweepOnlyTouchesReloadableResources(@TempDir Path dir) throws Exception {
        AtomicInteger built = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rs.db").toString()).open()) {
            queue.resource("hot", counted("hot", built, disposed, true));
            queue.resource("cold", counted("cold", built, disposed, false));

            try (Worker worker = queue.worker().handle(TASK, payload -> payload).start()) {
                assertEquals(Map.of("hot", true), queue.reloadResources());
                // Naming it explicitly reloads it whatever the flag says.
                assertEquals(Map.of("cold", true), queue.reloadResources(List.of("cold")));
            }
        }
    }

    @Test
    @Timeout(30)
    void unknownNameReportsFalseRatherThanThrowing(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("ru.db").toString()).open()) {
            queue.resource("db", ctx -> new Object());
            try (Worker worker = queue.worker().handle(TASK, payload -> payload).start()) {
                assertEquals(Map.of("nope", false), queue.reloadResources(List.of("nope")));
            }
        }
    }

    @Test
    void reloadWithoutARunningWorkerIsEmpty(@TempDir Path dir) {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rn.db").toString()).open()) {
            queue.resource("db", ResourceDefinition.worker(ctx -> new Object()).withReloadable(true));
            // Instances live in the per-worker runtimes; with none running there is
            // nothing cached to replace.
            assertTrue(queue.reloadResources().isEmpty());
            assertTrue(queue.reloadResources(List.of("db")).isEmpty());
        }
    }

    @Test
    @Timeout(30)
    void reloadRebuildsADependencyBeforeItsDependent(@TempDir Path dir) throws Exception {
        List<String> order = new CopyOnWriteArrayList<>();
        AtomicReference<String> outerSaw = new AtomicReference<>();
        AtomicInteger innerBuilds = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rd.db").toString()).open()) {
            queue.resource("inner", ctx -> {
                order.add("inner");
                return "inner-" + innerBuilds.incrementAndGet();
            });
            queue.resource("outer", ctx -> {
                order.add("outer");
                String inner = ctx.use("inner");
                outerSaw.set(inner);
                return "outer(" + inner + ")";
            });

            CountDownLatch ran = new CountDownLatch(1);
            try (Worker worker = queue.worker()
                    .handle(TASK, payload -> {
                        Resources.use("outer");
                        ran.countDown();
                        return payload;
                    })
                    .start()) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS), "handler did not run");
                assertEquals("inner-1", outerSaw.get());

                order.clear();
                Map<String, Boolean> results = queue.reloadResources(List.of("outer", "inner"));

                assertEquals(Map.of("inner", true, "outer", true), results);
                assertEquals(List.of("inner", "outer"), order, "the dependency rebuilds first");
                assertEquals("inner-2", outerSaw.get(), "the dependent picked up the fresh instance");
            }
        }
    }

    @Test
    @Timeout(30)
    void aFailingFactoryReportsFalse(@TempDir Path dir) throws Exception {
        AtomicInteger builds = new AtomicInteger();
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rf.db").toString()).open()) {
            queue.resource("flaky", ctx -> {
                if (builds.incrementAndGet() > 1) {
                    throw new IllegalStateException("cannot rebuild");
                }
                return new Object();
            });

            CountDownLatch ran = new CountDownLatch(1);
            try (Worker worker = queue.worker()
                    .handle(TASK, payload -> {
                        Resources.use("flaky");
                        ran.countDown();
                        return payload;
                    })
                    .start()) {
                queue.enqueue(TASK, 1);
                assertTrue(ran.await(20, TimeUnit.SECONDS), "handler did not run");
                assertFalse(queue.reloadResources(List.of("flaky")).get("flaky"));
            }
        }
    }
}
