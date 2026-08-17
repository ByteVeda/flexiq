package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowSagaTest {

    private static final Task<Integer> A = Task.of("sg.a", Integer.class);
    private static final Task<Integer> B = Task.of("sg.b", Integer.class);
    private static final Task<Integer> C = Task.of("sg.c", Integer.class);
    private static final Task<Integer> UNDO_A = Task.of("sg.undoA", Integer.class);
    private static final Task<Integer> UNDO_B = Task.of("sg.undoB", Integer.class);

    @Test
    @Timeout(30)
    void compensatesCompletedStepsOnFailure(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sg.db").toString()).open()) {
            Workflow wf = Workflow.named("saga")
                    .step(Step.of("a", A, 1).compensate(UNDO_A).build())
                    .step(Step.of("b", B, 2).after("a").compensate(UNDO_B).build())
                    .step(Step.of("c", C, 3).after("b").maxRetries(0).build());

            WorkflowRun run = queue.submitWorkflow(wf);
            Queue<Integer> undone = new ConcurrentLinkedQueue<>();
            Worker worker = queue.worker()
                    .handle(A, p -> p)
                    .handle(B, p -> p)
                    .handle(C, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .handle(UNDO_A, p -> {
                        undone.add(100 + p);
                        return p;
                    })
                    .handle(UNDO_B, p -> {
                        undone.add(200 + p);
                        return p;
                    })
                    .trackWorkflows()
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPENSATED, status.state);
                assertEquals(NodeStatus.COMPENSATED, status.node("a").orElseThrow().status);
                assertEquals(NodeStatus.COMPENSATED, status.node("b").orElseThrow().status);
                assertEquals(NodeStatus.FAILED, status.node("c").orElseThrow().status);
                // Reverse-dependency order, each exactly once: b rolls back before a
                // (undoB saw b's result 2 → 202; undoA saw a's result 1 → 101).
                assertEquals(List.of(202, 101), List.copyOf(undone));
            }
        }
    }

    @Test
    @Timeout(30)
    void emitsCompensationLifecycleInOrder(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sgev.db").toString()).open()) {
            List<EventName> seen = new CopyOnWriteArrayList<>();
            for (EventName name : List.of(
                    EventName.WORKFLOW_COMPENSATING,
                    EventName.WORKFLOW_NODE_COMPENSATING,
                    EventName.WORKFLOW_NODE_COMPENSATED,
                    EventName.WORKFLOW_COMPENSATED)) {
                queue.onEvent(name, event -> seen.add(event.name()));
            }
            Workflow wf = Workflow.named("sagaev")
                    .step(Step.of("a", A, 1).compensate(UNDO_A).build())
                    .step(Step.of("c", C, 3).after("a").maxRetries(0).build());

            WorkflowRun run = queue.submitWorkflow(wf);
            Worker worker = queue.worker()
                    .handle(A, p -> p)
                    .handle(C, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .handle(UNDO_A, p -> p)
                    .trackWorkflows()
                    .start();
            try (worker) {
                assertEquals(WorkflowState.COMPENSATED, run.await(Duration.ofSeconds(20)).state);
                // The final event lands just after the run turns terminal; poll for it.
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (seen.size() < 4 && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                if (seen.size() < 4) {
                    fail("expected the full compensation lifecycle, saw " + seen);
                }
                assertTrue(
                        seen.indexOf(EventName.WORKFLOW_COMPENSATING)
                                < seen.indexOf(EventName.WORKFLOW_NODE_COMPENSATING),
                        "run compensating must precede node compensating: " + seen);
                assertTrue(
                        seen.indexOf(EventName.WORKFLOW_NODE_COMPENSATING)
                                < seen.indexOf(EventName.WORKFLOW_NODE_COMPENSATED),
                        "node compensating must precede node compensated: " + seen);
                assertTrue(
                        seen.indexOf(EventName.WORKFLOW_NODE_COMPENSATED)
                                < seen.indexOf(EventName.WORKFLOW_COMPENSATED),
                        "node compensated must precede run compensated: " + seen);
            }
        }
    }

    @Test
    @Timeout(30)
    void noCompensatorsStillFailsNormally(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sg2.db").toString()).open()) {
            Workflow wf = Workflow.named("plain")
                    .step("a", A, 1)
                    .step(Step.of("b", B, 2).after("a").maxRetries(0).build());

            WorkflowRun run = queue.submitWorkflow(wf);
            Worker worker = queue.worker()
                    .handle(A, p -> p)
                    .handle(B, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .trackWorkflows()
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.FAILED, status.state);
            }
        }
    }
}
