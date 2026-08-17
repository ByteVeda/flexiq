package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.GateEvent;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowGateTest {

    private static final Task<Integer> PROCESS = Task.of("g.process", Integer.class);
    private static final Task<Integer> DEPLOY = Task.of("g.deploy", Integer.class);

    private static Workflow gatedWorkflow(GateConfig gate) {
        return Workflow.named("gated")
                .step("process", PROCESS, 1)
                .gate("gate", gate, "process")
                .step("deploy", DEPLOY, 2, "gate");
    }

    @Test
    @Timeout(30)
    void approvedGateRunsDownstream(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("g.db").toString()).open()) {
            Workflow wf = gatedWorkflow(GateConfig.manual());
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicInteger deployed = new AtomicInteger();
            try (Worker worker = queue.worker()
                    .handle(PROCESS, p -> p)
                    .handle(DEPLOY, p -> deployed.incrementAndGet())
                    .trackWorkflows(wf)
                    .start()) {
                awaitGate(run, NodeStatus.WAITING_APPROVAL);
                worker.approveGate(run.runId(), "gate");

                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);
                assertEquals(NodeStatus.COMPLETED, status.node("gate").orElseThrow().status);
                assertEquals(NodeStatus.COMPLETED, status.node("deploy").orElseThrow().status);
                assertEquals(1, deployed.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void rejectedGateSkipsDownstream(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("g.db").toString()).open()) {
            Workflow wf = gatedWorkflow(GateConfig.manual());
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicInteger deployed = new AtomicInteger();
            try (Worker worker = queue.worker()
                    .handle(PROCESS, p -> p)
                    .handle(DEPLOY, p -> deployed.incrementAndGet())
                    .trackWorkflows(wf)
                    .start()) {
                awaitGate(run, NodeStatus.WAITING_APPROVAL);
                worker.rejectGate(run.runId(), "gate", "no");

                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.FAILED, status.state);
                assertEquals(NodeStatus.FAILED, status.node("gate").orElseThrow().status);
                assertEquals(NodeStatus.SKIPPED, status.node("deploy").orElseThrow().status);
                assertEquals(0, deployed.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void gateTimeoutAutoApproves(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("g.db").toString()).open()) {
            Workflow wf = gatedWorkflow(GateConfig.timeout(Duration.ofMillis(300), GateAction.APPROVE));
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicInteger deployed = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(PROCESS, p -> p)
                    .handle(DEPLOY, p -> deployed.incrementAndGet())
                    .trackWorkflows(wf)
                    .start();
            try (worker) {
                // No manual resolution — the timeout drives it to approval.
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);
                assertEquals(1, deployed.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void gateTimeoutAutoRejects(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("g.db").toString()).open()) {
            Workflow wf = gatedWorkflow(GateConfig.timeout(Duration.ofMillis(300), GateAction.REJECT));
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicInteger deployed = new AtomicInteger();
            Worker worker = queue.worker()
                    .handle(PROCESS, p -> p)
                    .handle(DEPLOY, p -> deployed.incrementAndGet())
                    .trackWorkflows(wf)
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.FAILED, status.state);
                assertEquals(NodeStatus.SKIPPED, status.node("deploy").orElseThrow().status);
                assertEquals(0, deployed.get());
            }
        }
    }

    @Test
    @Timeout(30)
    void emitsGateReachedWhenTheGateParks(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("gev.db").toString()).open()) {
            List<GateEvent> seen = new CopyOnWriteArrayList<>();
            queue.onEvent(EventName.WORKFLOW_GATE_REACHED, event -> seen.add((GateEvent) event));

            Workflow wf = gatedWorkflow(GateConfig.manual());
            WorkflowRun run = queue.submitWorkflow(wf);
            AtomicInteger deployed = new AtomicInteger();
            try (Worker worker = queue.worker()
                    .handle(PROCESS, p -> p)
                    .handle(DEPLOY, p -> deployed.incrementAndGet())
                    .trackWorkflows(wf)
                    .start()) {
                // The event is emitted just after the node is parked; poll for it.
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (seen.isEmpty() && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                assertEquals(List.of(new GateEvent(run.runId(), "gate")), seen);
                worker.approveGate(run.runId(), "gate");
                assertEquals(WorkflowState.COMPLETED, run.await(Duration.ofSeconds(20)).state);
            }
        }
    }

    /** Poll until the gate node reaches {@code expected}, or fail after 10s. */
    private static void awaitGate(WorkflowRun run, NodeStatus expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            Optional<NodeSnapshot> gate = run.status().flatMap(s -> s.node("gate"));
            if (gate.isPresent() && gate.get().status == expected) {
                return;
            }
            Thread.sleep(50);
        }
        fail("gate did not reach " + expected);
    }
}
