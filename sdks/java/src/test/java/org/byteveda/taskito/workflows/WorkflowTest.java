package org.byteveda.taskito.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.events.WorkflowEvent;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowTest {

    @Test
    @Timeout(30)
    void runsLinearDagToCompletion(@TempDir Path dir) throws Exception {
        Task<Integer> a = Task.of("wf.a", Integer.class);
        Task<Integer> b = Task.of("wf.b", Integer.class);
        Task<Integer> c = Task.of("wf.c", Integer.class);
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("wf.db").toString()).open()) {
            Workflow wf = Workflow.named("pipeline")
                    .step("a", a, 1)
                    .step("b", b, 2, "a")
                    .step("c", c, 3, "b");

            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicInteger ran = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(a, p -> ran.incrementAndGet())
                    .handle(b, p -> ran.incrementAndGet())
                    .handle(c, p -> ran.incrementAndGet())
                    .trackWorkflows()
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);
                assertEquals(3, ran.get());
                assertEquals(NodeStatus.COMPLETED, status.node("c").orElseThrow().status);
            }
        }
    }

    @Test
    @Timeout(30)
    void failingNodeFailsRunAndSkipsDownstream(@TempDir Path dir) throws Exception {
        Task<Integer> a = Task.of("fc.a", Integer.class);
        Task<Integer> b = Task.of("fc.b", Integer.class);
        Task<Integer> c = Task.of("fc.c", Integer.class);
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("fc.db").toString()).open()) {
            Workflow wf = Workflow.named("failpipe")
                    .step("a", a, 1)
                    .step(Step.of("b", b, 2).after("a").maxRetries(0).build())
                    .step("c", c, 3, "b");

            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicInteger cRan = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(a, p -> p)
                    .handle(b, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .handle(c, p -> cRan.incrementAndGet())
                    .trackWorkflows()
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.FAILED, status.state);
                assertEquals(NodeStatus.FAILED, status.node("b").orElseThrow().status);
                assertEquals(NodeStatus.SKIPPED, status.node("c").orElseThrow().status);
                assertEquals(0, cRan.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void emitsWorkflowSubmittedAndCompleted(@TempDir Path dir) throws Exception {
        Task<Integer> a = Task.of("evwf.a", Integer.class);
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("evwf.db").toString()).open()) {
            List<WorkflowEvent> seen = new CopyOnWriteArrayList<>();
            queue.onEvent(EventName.WORKFLOW_SUBMITTED, event -> seen.add((WorkflowEvent) event));
            queue.onEvent(EventName.WORKFLOW_COMPLETED, event -> seen.add((WorkflowEvent) event));

            Workflow wf = Workflow.named("evpipeline").step("a", a, 1);
            WorkflowRun run = queue.submitWorkflow(wf);
            Worker worker = queue.worker().handle(a, p -> p).trackWorkflows().start();
            try (worker) {
                run.await(Duration.ofSeconds(20));
                // The completion event lands just after the run turns terminal.
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (seen.size() < 2 && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                if (seen.size() < 2) {
                    fail("expected submitted + completed events, saw " + seen);
                }
                assertEquals(
                        new WorkflowEvent(EventName.WORKFLOW_SUBMITTED, run.runId(), "evpipeline", null), seen.get(0));
                assertEquals(
                        new WorkflowEvent(EventName.WORKFLOW_COMPLETED, run.runId(), "evpipeline", null), seen.get(1));
            }
        }
    }

    @Test
    @Timeout(30)
    void cancelSkipsPendingNodes(@TempDir Path dir) {
        Task<Integer> a = Task.of("cx.a", Integer.class);
        Task<Integer> b = Task.of("cx.b", Integer.class);
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("cx.db").toString()).open()) {
            Workflow wf = Workflow.named("cancelpipe").step("a", a, 1).step("b", b, 2, "a");
            WorkflowRun run = queue.submitWorkflow(wf);
            run.cancel();

            WorkflowStatus status = run.status().orElseThrow();
            assertEquals(WorkflowState.CANCELLED, status.state);
            assertTrue(status.node("a").orElseThrow().status == NodeStatus.SKIPPED);
            assertTrue(status.node("b").orElseThrow().status == NodeStatus.SKIPPED);
        }
    }
}
