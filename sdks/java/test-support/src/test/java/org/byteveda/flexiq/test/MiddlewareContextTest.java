package org.byteveda.flexiq.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.middleware.EnqueueContext;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class MiddlewareContextTest {

    @Test
    @Timeout(20)
    void metadataTravelsAndAttributesAreShared() throws Exception {
        AtomicReference<String> afterSaw = new AtomicReference<>();
        Task<Integer> echo = Task.of("mw.echo", Integer.class);
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.use(new Middleware() {
                @Override
                public void onEnqueue(EnqueueContext ctx) {
                    ctx.metadata().put("trace-id", "trace-123"); // injected at enqueue
                }

                @Override
                public void before(TaskContext ctx) {
                    Object traceId = ctx.job().metadata().get("trace-id"); // read at execution
                    ctx.attributes().put("seen", traceId); // scratch shared with after()
                }

                @Override
                public void after(TaskContext ctx, Object result) {
                    afterSaw.set((String) ctx.attributes().get("seen"));
                }
            });

            String id = queue.enqueue(echo, 7);
            Worker worker = queue.worker().handle(echo, p -> p).start();
            try (worker) {
                queue.awaitJob(id, Duration.ofSeconds(10));
            }
            assertEquals("trace-123", afterSaw.get());
        }
    }
}
