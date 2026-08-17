package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowConditionTest {

    private static final Task<Integer> RISKY = Task.of("c.risky", Integer.class);
    private static final Task<String> RECOVER = Task.of("c.recover", String.class);
    private static final Task<Integer> PRODUCER = Task.of("c.producer", Integer.class);
    private static final Task<Integer> CHECK = Task.of("c.check", Integer.class);

    @Test
    @Timeout(30)
    void onFailureRunsRecoveryNode(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("c.db").toString()).open()) {
            Workflow wf = Workflow.named("cond")
                    .step(Step.of("risky", RISKY, 1).maxRetries(0).build())
                    .step(Step.of("recover", RECOVER, "fix")
                            .onFailure()
                            .after("risky")
                            .build());
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicBoolean recovered = new AtomicBoolean();
            Worker worker = queue.worker()
                    .handle(RISKY, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .handle(RECOVER, p -> {
                        recovered.set(true);
                        return p;
                    })
                    .trackWorkflows(wf)
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.FAILED, status.state);
                assertEquals(NodeStatus.FAILED, status.node("risky").orElseThrow().status);
                assertEquals(NodeStatus.COMPLETED, status.node("recover").orElseThrow().status);
                assertEquals(true, recovered.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void onSuccessSkipsRecoveryNode(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("c.db").toString()).open()) {
            Workflow wf = Workflow.named("cond")
                    .step("risky", RISKY, 1)
                    .step(Step.of("recover", RECOVER, "fix")
                            .onFailure()
                            .after("risky")
                            .build());
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicBoolean recovered = new AtomicBoolean();
            Worker worker = queue.worker()
                    .handle(RISKY, p -> p)
                    .handle(RECOVER, p -> {
                        recovered.set(true);
                        return p;
                    })
                    .trackWorkflows(wf)
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);
                assertEquals(NodeStatus.SKIPPED, status.node("recover").orElseThrow().status);
                assertEquals(false, recovered.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void alwaysRunsAfterFailure(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("c.db").toString()).open()) {
            Workflow wf = Workflow.named("cond")
                    .step(Step.of("risky", RISKY, 1).maxRetries(0).build())
                    .step(Step.of("cleanup", RECOVER, "clean")
                            .always()
                            .after("risky")
                            .build());
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicBoolean cleaned = new AtomicBoolean();
            Worker worker = queue.worker()
                    .handle(RISKY, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .handle(RECOVER, p -> {
                        cleaned.set(true);
                        return p;
                    })
                    .trackWorkflows(wf)
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.FAILED, status.state);
                assertEquals(NodeStatus.COMPLETED, status.node("cleanup").orElseThrow().status);
                assertEquals(true, cleaned.get());
            }
        }
    }

    @Test
    void conditionEnumOverloadMatchesStringForm() {
        assertEquals("on_success", WorkflowCondition.ON_SUCCESS.wire());
        assertEquals(WorkflowCondition.ALWAYS, WorkflowCondition.fromWire("always"));
        Step viaEnum = Step.of("s", RECOVER, "x")
                .condition(WorkflowCondition.ON_FAILURE)
                .build();
        Step viaString = Step.of("s", RECOVER, "x").onFailure().build();
        assertEquals(viaString.condition, viaEnum.condition);
        assertEquals("on_failure", viaEnum.condition);
    }

    @Test
    @Timeout(30)
    void callableConditionRunsWhenTrue(@TempDir Path dir) throws Exception {
        assertCallable(dir, 10, true);
    }

    @Test
    @Timeout(30)
    void callableConditionSkipsWhenFalse(@TempDir Path dir) throws Exception {
        assertCallable(dir, 3, false);
    }

    private static void assertCallable(Path dir, int produced, boolean expectChecked) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("c.db").toString()).open()) {
            Workflow wf = Workflow.named("cond")
                    .step("producer", PRODUCER, produced)
                    .step(Step.of("check", CHECK, 0)
                            .condition(ctx -> ((Number) ctx.result("producer").orElse(0)).intValue() > 5)
                            .after("producer")
                            .build());
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicBoolean checked = new AtomicBoolean();
            Worker worker = queue.worker()
                    .handle(PRODUCER, p -> p)
                    .handle(CHECK, p -> {
                        checked.set(true);
                        return p;
                    })
                    .trackWorkflows(wf)
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);
                assertEquals(
                        expectChecked ? NodeStatus.COMPLETED : NodeStatus.SKIPPED,
                        status.node("check").orElseThrow().status);
                assertEquals(expectChecked, checked.get());
            }
        }
    }
}
