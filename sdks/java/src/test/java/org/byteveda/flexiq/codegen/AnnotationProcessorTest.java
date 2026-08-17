package org.byteveda.flexiq.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class AnnotationProcessorTest {

    @Test
    @Timeout(30)
    void generatedCompanionEnqueuesAndHandles(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("ap.db").toString()).open()) {
            // GreeterTasks is generated from @TaskHandler on Greeter; GREET/TOTAL
            // are typed Task constants (TOTAL carries the generic List<Integer>).
            String greetId = queue.enqueue(GreeterTasks.GREET, "ada");
            String totalId = queue.enqueue(GreeterTasks.TOTAL, List.of(1, 2, 3, 4));

            CountDownLatch done = new CountDownLatch(2);
            Worker worker = queue.worker()
                    .apply(builder -> GreeterTasks.bind(builder, new Greeter()))
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "both tasks should complete");
                assertEquals("hello ada", queue.getResult(greetId, String.class).orElseThrow());
                assertEquals(10, queue.getResult(totalId, Integer.class).orElseThrow());
            }
        }
    }

    @Test
    @Timeout(30)
    void injectsResourceParameter(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("ri.db").toString()).open()) {
            // ResourceGreeter.greet takes a @Resource("salutation") param; the generated
            // bind resolves it from the worker resource runtime.
            queue.resource("salutation", ctx -> "hi");
            String id = queue.enqueue(ResourceGreeterTasks.GREET, "ada");
            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .apply(builder -> ResourceGreeterTasks.bind(builder, new ResourceGreeter()))
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "task should complete");
                assertEquals("hi ada", queue.getResult(id, String.class).orElseThrow());
            }
        }
    }

    @Test
    @Timeout(30)
    void registerViaGeneratedHandlerRegistry(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("hr.db").toString()).open()) {
            String id = queue.enqueue(GreeterTasks.GREET, "grace");
            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .register(GreeterTasks.handlers(new Greeter())) // generated HandlerRegistry
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "task should complete");
                assertEquals("hello grace", queue.getResult(id, String.class).orElseThrow());
            }
        }
    }
}
