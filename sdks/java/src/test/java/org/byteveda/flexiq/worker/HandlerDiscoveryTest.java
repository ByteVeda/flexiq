package org.byteveda.flexiq.worker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.codegen.GreeterTasks;
import org.byteveda.flexiq.errors.DuplicateTaskException;
import org.byteveda.flexiq.errors.TaskDiscoveryException;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code META-INF/services} path the {@code @TaskHandler} processor generates:
 * handlers reach a worker without any application code naming them.
 */
class HandlerDiscoveryTest {

    private static final String SERVICE = "META-INF/services/org.byteveda.flexiq.worker.HandlerRegistryProvider";

    @Test
    @Timeout(30)
    void runsAGeneratedHandlerNothingRegistered(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("d.db").toString()).open()) {
            // GreeterTasks$Provider is listed in META-INF/services by the processor;
            // the builder never sees Greeter, GREET, or a handler function.
            String id = queue.enqueue(GreeterTasks.GREET, "ada");

            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .discover()
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "the discovered handler should run the task");
                assertEquals("hello ada", queue.getResult(id, String.class).orElseThrow());
            }
        }
    }

    @Test
    void emptyServiceFileDiscoversNothing(@TempDir Path dir) throws Exception {
        assertTrue(HandlerDiscovery.load(loaderFor(dir, "")).isEmpty());
    }

    @Test
    void twoProvidersClaimingOneNameFail(@TempDir Path dir) throws Exception {
        ClassLoader loader = loaderFor(dir, First.class.getName() + "\n" + Second.class.getName());

        DuplicateTaskException error = assertThrows(DuplicateTaskException.class, () -> HandlerDiscovery.load(loader));

        // Both sides named: knowing only the task is not enough to find the shadowing jar.
        assertTrue(error.getMessage().contains("clash"), error.getMessage());
        assertTrue(error.getMessage().contains(First.class.getName()), error.getMessage());
        assertTrue(error.getMessage().contains(Second.class.getName()), error.getMessage());
    }

    @Test
    void aProviderThatCannotLoadNamesItself(@TempDir Path dir) throws Exception {
        ClassLoader loader = loaderFor(dir, "org.byteveda.flexiq.NoSuchProvider");

        TaskDiscoveryException error = assertThrows(TaskDiscoveryException.class, () -> HandlerDiscovery.load(loader));

        assertTrue(error.getMessage().contains("org.byteveda.flexiq.NoSuchProvider"), error.getMessage());
    }

    @Test
    void aProviderThatThrowsNamesItself(@TempDir Path dir) throws Exception {
        ClassLoader loader = loaderFor(dir, Broken.class.getName());

        TaskDiscoveryException error = assertThrows(TaskDiscoveryException.class, () -> HandlerDiscovery.load(loader));

        assertTrue(error.getMessage().contains(Broken.class.getName()), error.getMessage());
        assertEquals("no database for you", error.getCause().getMessage());
    }

    @Test
    @Timeout(30)
    void discoveryDoesNotReplaceAnExplicitHandler(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("x.db").toString()).open()) {
            Worker.Builder builder = queue.worker().handle(GreeterTasks.GREET, (String name) -> "mine");

            assertThrows(DuplicateTaskException.class, builder::discover);
        }
    }

    @Test
    @Timeout(30)
    void registeringAfterDiscoveryOverrides(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("o.db").toString()).open()) {
            String id = queue.enqueue(GreeterTasks.GREET, "ada");

            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .discover()
                    .handle(GreeterTasks.GREET, (String name) -> "overridden " + name)
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "the task should complete");
                assertEquals("overridden ada", queue.getResult(id, String.class).orElseThrow());
            }
        }
    }

    /**
     * A loader serving exactly {@code entries} for the provider service, so a test
     * sees only its own providers and not the ones the processor generated for the
     * test fixtures.
     */
    private static ClassLoader loaderFor(Path dir, String entries) throws IOException {
        Path file = Files.createDirectories(dir.resolve("services")).resolve("providers");
        Files.write(file, entries.getBytes(UTF_8));
        URL url = file.toUri().toURL();
        return new ClassLoader(HandlerDiscoveryTest.class.getClassLoader()) {
            @Override
            public Enumeration<URL> getResources(String name) throws IOException {
                return SERVICE.equals(name) ? Collections.enumeration(List.of(url)) : super.getResources(name);
            }
        };
    }

    /** Both of these claim {@code clash}; only one can win, so neither may. */
    public static final class First implements HandlerRegistryProvider {
        public First() {}

        @Override
        public HandlerRegistry registry() {
            return HandlerRegistry.of(Handler.of(Task.of("clash", String.class), (String s) -> s));
        }
    }

    /** The second claimant of {@code clash}. */
    public static final class Second implements HandlerRegistryProvider {
        public Second() {}

        @Override
        public HandlerRegistry registry() {
            return HandlerRegistry.of(Handler.of(Task.of("clash", String.class), (String s) -> s));
        }
    }

    /** A provider whose handlers cannot be built — the "11 of 12 modules" failure. */
    public static final class Broken implements HandlerRegistryProvider {
        public Broken() {}

        @Override
        public HandlerRegistry registry() {
            throw new IllegalStateException("no database for you");
        }
    }
}
