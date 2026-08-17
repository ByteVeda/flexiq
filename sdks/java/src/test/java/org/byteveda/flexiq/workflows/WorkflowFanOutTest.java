package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class WorkflowFanOutTest {

    @Test
    @Timeout(30)
    void fansOutPerItemAndFansInResults(@TempDir Path dir) throws Exception {
        Task<Integer> seed = Task.of("fo.seed", Integer.class);
        Task<Integer> square = Task.of("fo.square", Integer.class);
        Task<List<Integer>> sum = Task.of("fo.sum", new TypeReference<List<Integer>>() {});
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("fo.db").toString()).open()) {
            // seed(4) -> [1,2,3,4]; square each -> [1,4,9,16]; sum all -> 30
            Workflow wf = Workflow.named("fanpipe")
                    .step("seed", seed, 4)
                    .fanOut("square", square, "each", "seed")
                    .fanIn("sum", sum, "all", "square");

            WorkflowRun run = queue.submitWorkflow(wf);
            Worker worker = queue.worker()
                    .handle(seed, n -> {
                        List<Integer> out = new ArrayList<>();
                        for (int i = 1; i <= n; i++) {
                            out.add(i);
                        }
                        return out;
                    })
                    .handle(square, x -> x * x)
                    .handle(sum, xs -> {
                        int total = 0;
                        for (Integer x : xs) {
                            total += x;
                        }
                        return total;
                    })
                    .trackWorkflows()
                    .start();
            try (worker) {
                WorkflowStatus status = run.await(Duration.ofSeconds(20));
                assertEquals(WorkflowState.COMPLETED, status.state);
                assertEquals(NodeStatus.COMPLETED, status.node("square").orElseThrow().status);
                assertEquals(NodeStatus.COMPLETED, status.node("sum").orElseThrow().status);

                long children = status.nodes.stream()
                        .filter(node -> node.nodeName.startsWith("square["))
                        .count();
                assertEquals(4, children);

                String sumJob = status.node("sum").orElseThrow().jobId;
                assertEquals(30, queue.getResult(sumJob, Integer.class).orElseThrow());
            }
        }
    }
}
