package org.byteveda.taskito.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.WorkflowException;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowCacheTest {

    private static final Task<Integer> SEED = Task.of("wc.seed", Integer.class);
    private static final Task<Integer> COMPUTE = Task.of("wc.compute", Integer.class);
    private static final Task<Integer> AFTER = Task.of("wc.after", Integer.class);

    @Test
    @Timeout(40)
    void cachedStepIsReusedOnRerun(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("wc.db").toString()).open()) {
            // compute is cacheable; it has a predecessor (a deferred root is never promoted).
            Workflow wf = Workflow.named("cached")
                    .step("seed", SEED, 0)
                    .step(Step.of("compute", COMPUTE, 7)
                            .cache(Duration.ofMinutes(5))
                            .after("seed")
                            .build())
                    .step("after", AFTER, 1, "compute");

            AtomicInteger computeRuns = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(SEED, p -> p)
                    .handle(COMPUTE, p -> computeRuns.incrementAndGet())
                    .handle(AFTER, p -> p)
                    .trackWorkflows(wf)
                    .start();
            try (worker) {
                // First run executes compute.
                WorkflowStatus first = queue.submitWorkflow(wf).await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, first.state);
                assertEquals(NodeStatus.COMPLETED, first.node("compute").orElseThrow().status);

                // Second run reuses it: compute is a cache hit, not re-executed.
                WorkflowRun run2 = queue.submitWorkflow(wf);
                WorkflowStatus second = run2.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, second.state);
                assertEquals(NodeStatus.CACHE_HIT, second.node("compute").orElseThrow().status);
                assertEquals(NodeStatus.COMPLETED, second.node("after").orElseThrow().status);
                assertEquals(1, computeRuns.get());
            }
        }
    }

    @Test
    void cacheableRootStepIsRejected() {
        // A deferred root is never promoted, so a cacheable step with no predecessor would hang.
        assertThrows(
                IllegalArgumentException.class,
                () -> Step.of("root", COMPUTE, 1).cache(Duration.ofMinutes(1)).build());
    }

    @Test
    void cachedFanProducerIsRejected(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("wc2.db").toString()).open()) {
            // A cache hit yields no result list, so a fan-out over a cached step can't expand.
            Workflow wf = Workflow.named("badcache")
                    .step("seed", SEED, 0)
                    .step(Step.of("producer", COMPUTE, 1)
                            .cache(Duration.ofMinutes(5))
                            .after("seed")
                            .build())
                    .step(Step.of("fan", AFTER)
                            .fanOut(FanMode.EACH)
                            .after("producer")
                            .build());
            assertThrows(WorkflowException.class, () -> queue.submitWorkflow(wf));
        }
    }
}
