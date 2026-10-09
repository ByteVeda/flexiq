package org.byteveda.flexiq.contrib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.WorkflowSubmitContext;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.byteveda.flexiq.workflows.Step;
import org.byteveda.flexiq.workflows.Workflow;
import org.byteveda.flexiq.workflows.WorkflowRun;
import org.byteveda.flexiq.workflows.WorkflowState;
import org.byteveda.flexiq.workflows.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * A workflow's steps join the trace of whoever submitted it: the submit's context
 * is stored on the run and every node job — pre-enqueued, fanned out, released
 * later by the tracker, or compensating — carries it, so each step's execute span
 * is a child of the submit span (flat, not chained).
 */
class WorkflowTraceTest {
    @RegisterExtension
    static final OpenTelemetryExtension OTEL = OpenTelemetryExtension.create();

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private static final Task<Integer> FIRST = Task.of("wt.first", Integer.class);
    private static final Task<Integer> SECOND = Task.of("wt.second", Integer.class);
    private static final Task<Integer> SEED = Task.of("wt.seed", Integer.class);
    private static final Task<Integer> DOUBLE = Task.of("wt.double", Integer.class);
    private static final Task<List<Integer>> SUM = Task.of("wt.sum", new TypeReference<List<Integer>>() {});
    private static final Task<Integer> CHARGE = Task.of("wt.charge", Integer.class);
    private static final Task<Integer> SHIP = Task.of("wt.ship", Integer.class);
    private static final Task<Integer> REFUND = Task.of("wt.refund", Integer.class);

    private static FlexiQ traced(Path dir) {
        FlexiQ queue = FlexiQ.builder().url(dir.resolve("wt.db").toString()).open();
        queue.use(new FlexiQOtel(OTEL.getOpenTelemetry()));
        return queue;
    }

    /** Submit under an active {@code submit} span, returning the run and that span. */
    private static Submitted inSubmit(Supplier<WorkflowRun> submit) {
        Span span =
                OTEL.getOpenTelemetry().getTracer("test").spanBuilder("submit").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            return new Submitted(submit.get(), span.getSpanContext());
        } finally {
            span.end();
        }
    }

    private record Submitted(WorkflowRun run, SpanContext span) {}

    /** The execute spans of {@code task}, once {@code count} of them have ended. */
    private static List<SpanData> executions(Task<?> task, int count) throws InterruptedException {
        String name = "flexiq.execute." + task.name();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (true) {
            List<SpanData> spans = OTEL.getSpans().stream()
                    .filter(span -> span.getName().equals(name))
                    .toList();
            if (spans.size() >= count || System.nanoTime() > deadline) {
                assertEquals(count, spans.size(), "execute spans of " + task.name());
                return spans;
            }
            Thread.sleep(20);
        }
    }

    private static void assertChildrenOf(List<SpanData> spans, SpanContext submit) {
        assertFalse(spans.isEmpty());
        for (SpanData span : spans) {
            assertEquals(submit.getTraceId(), span.getTraceId(), span.getName());
            assertEquals(submit.getSpanId(), span.getParentSpanId(), span.getName());
        }
    }

    @Test
    @Timeout(30)
    void everyStepIsAChildOfTheSubmitSpan(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = traced(dir)) {
            Workflow wf = Workflow.named("traced-linear")
                    .step("a", FIRST, 1)
                    .step(Step.of("b", SECOND, 2).after("a").build());
            Submitted submitted = inSubmit(() -> queue.submitWorkflow(wf));
            try (Worker worker = queue.worker()
                    .handle(FIRST, p -> p)
                    .handle(SECOND, p -> p)
                    .trackWorkflows()
                    .start()) {
                assertEquals(WorkflowState.COMPLETED, submitted.run().await(Duration.ofSeconds(20)).state);
                List<SpanData> spans = new ArrayList<>(executions(FIRST, 1));
                spans.addAll(executions(SECOND, 1));
                assertChildrenOf(spans, submitted.span());
            }
        }
    }

    @Test
    @Timeout(30)
    void fanOutChildrenAndTheFanInJoinTheTrace(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = traced(dir)) {
            Workflow wf = Workflow.named("traced-fanout")
                    .step("seed", SEED, 2)
                    .fanOut("double", DOUBLE, "each", "seed")
                    .fanIn("sum", SUM, "all", "double");
            Submitted submitted = inSubmit(() -> queue.submitWorkflow(wf));
            try (Worker worker = queue.worker()
                    .handle(SEED, n -> List.of(1, n))
                    .handle(DOUBLE, x -> x * 2)
                    .handle(SUM, xs -> xs.stream().mapToInt(Integer::intValue).sum())
                    .trackWorkflows()
                    .start()) {
                assertEquals(WorkflowState.COMPLETED, submitted.run().await(Duration.ofSeconds(20)).state);
                List<SpanData> spans = new ArrayList<>(executions(DOUBLE, 2));
                spans.addAll(executions(SUM, 1));
                assertChildrenOf(spans, submitted.span());
            }
        }
    }

    @Test
    @Timeout(30)
    void aCompensationJoinsTheTrace(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = traced(dir)) {
            Workflow wf = Workflow.named("traced-saga")
                    .step(Step.of("charge", CHARGE, 1).compensate(REFUND).build())
                    .step(Step.of("ship", SHIP, 2).after("charge").maxRetries(0).build());
            Submitted submitted = inSubmit(() -> queue.submitWorkflow(wf));
            try (Worker worker = queue.worker()
                    .handle(CHARGE, p -> p)
                    .handle(SHIP, p -> {
                        throw new IllegalStateException("out of stock");
                    })
                    .handle(REFUND, p -> p)
                    .trackWorkflows()
                    .start()) {
                assertEquals(WorkflowState.COMPENSATED, submitted.run().await(Duration.ofSeconds(20)).state);
                assertChildrenOf(executions(REFUND, 1), submitted.span());
            }
        }
    }

    @Test
    @Timeout(30)
    void anExplicitTraceContextReachesEveryNodeJob(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("wt.db").toString()).open()) {
            Workflow wf = Workflow.named("explicit-trace")
                    .step("a", FIRST, 1)
                    .step(Step.of("b", SECOND, 2).after("a").build());
            WorkflowRun run =
                    queue.submitWorkflow(wf, Map.of(), Map.of("traceparent", TRACEPARENT, "tracestate", "vendor=a"));

            WorkflowStatus status = run.status().orElseThrow();
            for (var node : status.nodes) {
                Map<?, ?> metadata = JSON.readValue(queue.getJob(node.jobId).orElseThrow().metadata, Map.class);
                assertEquals(
                        Map.of(
                                "workflow_run_id",
                                run.runId(),
                                "workflow_node_name",
                                node.nodeName,
                                "traceparent",
                                TRACEPARENT,
                                "tracestate",
                                "vendor=a"),
                        metadata);
            }
        }
    }

    @Test
    @Timeout(30)
    void anExplicitTraceContextWinsOverTheMiddleware(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = traced(dir)) {
            Workflow wf = Workflow.named("explicit-wins").step("a", FIRST, 1);
            Submitted submitted =
                    inSubmit(() -> queue.submitWorkflow(wf, Map.of(), Map.of("traceparent", TRACEPARENT)));

            String jobId = submitted.run().status().orElseThrow().node("a").orElseThrow().jobId;
            Map<?, ?> metadata = JSON.readValue(queue.getJob(jobId).orElseThrow().metadata, Map.class);
            assertEquals(TRACEPARENT, metadata.get("traceparent"));
        }
    }

    @Test
    @Timeout(30)
    void aThrowingHookDoesNotBlockTheSubmit(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("wt.db").toString()).open()) {
            queue.use(new Middleware() {
                @Override
                public void onWorkflowSubmit(WorkflowSubmitContext context) {
                    throw new IllegalStateException("hook failed");
                }
            });
            Workflow wf = Workflow.named("hook-throws").step("a", FIRST, 1);
            WorkflowRun run = queue.submitWorkflow(wf, Map.of(), Map.of("traceparent", TRACEPARENT));

            String jobId = run.status().orElseThrow().node("a").orElseThrow().jobId;
            Map<?, ?> metadata = JSON.readValue(queue.getJob(jobId).orElseThrow().metadata, Map.class);
            assertEquals(TRACEPARENT, metadata.get("traceparent"));
        }
    }
}
