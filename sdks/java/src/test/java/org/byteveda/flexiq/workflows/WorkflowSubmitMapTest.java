package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowSubmitMapTest {

    @Test
    @Timeout(30)
    void structuralStepsWithPayloadsSuppliedAtSubmit(@TempDir Path dir) throws Exception {
        Task<Integer> extract = Task.of("sm.extract", Integer.class);
        Task<Integer> transform = Task.of("sm.transform", Integer.class);
        Task<Integer> load = Task.of("sm.load", Integer.class);
        try (FlexiQ queue =
                FlexiQ.builder().sqlite(dir.resolve("sm.db").toString()).open()) {
            // Structural steps (deps only); payloads supplied at submit.
            Workflow etl = Workflow.named("etl")
                    .stepAfter("extract", extract)
                    .stepAfter("transform", transform, "extract")
                    .stepAfter("load", load, "transform");

            WorkflowRun run = queue.submitWorkflow(etl, Map.of("extract", 5, "transform", 6, "load", 7));
            Worker worker = queue.worker()
                    .handle(extract, p -> p)
                    .handle(transform, p -> p)
                    .handle(load, p -> p)
                    .trackWorkflows()
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);

                // Each step received the payload supplied at submit.
                String loadJob = status.node("load").orElseThrow().jobId;
                assertEquals(7, queue.getResult(loadJob, Integer.class).orElseThrow());
            }
        }
    }
}
