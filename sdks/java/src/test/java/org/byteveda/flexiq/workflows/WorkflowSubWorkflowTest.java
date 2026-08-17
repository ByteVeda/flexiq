package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.FlexiQException;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowSubWorkflowTest {

    private static final Task<Integer> PREP = Task.of("sw.prep", Integer.class);
    private static final Task<Integer> CHILD_A = Task.of("sw.childA", Integer.class);
    private static final Task<Integer> CHILD_B = Task.of("sw.childB", Integer.class);
    private static final Task<Integer> FINISH = Task.of("sw.finish", Integer.class);

    @Test
    @Timeout(30)
    void childCompletesThenParentContinues(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sw.db").toString()).open()) {
            Workflow child = Workflow.named("child").step("a", CHILD_A, 10).step("b", CHILD_B, 20, "a");
            Workflow parent = Workflow.named("parent")
                    .step("prep", PREP, 1)
                    .subWorkflow("sub", child, "prep")
                    .step("finish", FINISH, 2, "sub");

            WorkflowRun run = queue.submitWorkflow(parent);
            AtomicInteger childRan = new AtomicInteger();
            AtomicInteger finished = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(PREP, p -> p)
                    .handle(CHILD_A, p -> childRan.incrementAndGet())
                    .handle(CHILD_B, p -> childRan.incrementAndGet())
                    .handle(FINISH, p -> finished.incrementAndGet())
                    .trackWorkflows(parent)
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);
                assertEquals(NodeStatus.COMPLETED, status.node("sub").orElseThrow().status);
                assertEquals(NodeStatus.COMPLETED, status.node("finish").orElseThrow().status);
                assertEquals(2, childRan.get());
                assertEquals(1, finished.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void childFailureFailsParentAndSkipsDownstream(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sw.db").toString()).open()) {
            Workflow child = Workflow.named("child")
                    .step(Step.of("a", CHILD_A, 10).maxRetries(0).build());
            Workflow parent = Workflow.named("parent")
                    .step("prep", PREP, 1)
                    .subWorkflow("sub", child, "prep")
                    .step("finish", FINISH, 2, "sub");

            WorkflowRun run = queue.submitWorkflow(parent);
            AtomicInteger finished = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(PREP, p -> p)
                    .handle(CHILD_A, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .handle(FINISH, p -> finished.incrementAndGet())
                    .trackWorkflows(parent)
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.FAILED, status.state);
                assertEquals(NodeStatus.FAILED, status.node("sub").orElseThrow().status);
                assertEquals(NodeStatus.SKIPPED, status.node("finish").orElseThrow().status);
                assertEquals(0, finished.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void rejectsChildStepsThatLoseStateAtSubmit(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("sw.db").toString()).open()) {
            // A child has no submit-time payload map: a payloadless plain task step can't run.
            Workflow noPayload =
                    Workflow.named("child").step(Step.of("a", CHILD_A).build());
            Workflow parentNoPayload = Workflow.named("p1").subWorkflow("sub", noPayload);
            assertThrows(FlexiQException.class, () -> queue.submitWorkflow(parentNoPayload));

            // A child has no callable registry across the submit boundary.
            Workflow callableChild = Workflow.named("child")
                    .step("a", CHILD_A, 1)
                    .step(Step.of("b", CHILD_B, 2)
                            .after("a")
                            .condition(ctx -> true)
                            .build());
            Workflow parentCallable = Workflow.named("p2").subWorkflow("sub", callableChild);
            assertThrows(FlexiQException.class, () -> queue.submitWorkflow(parentCallable));
        }
    }
}
