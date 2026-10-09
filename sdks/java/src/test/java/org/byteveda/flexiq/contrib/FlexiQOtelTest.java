package org.byteveda.flexiq.contrib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.middleware.EnqueueContext;
import org.byteveda.flexiq.middleware.JobInfo;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Trace context crosses the queue: enqueue injects, execution continues. */
class FlexiQOtelTest {
    @RegisterExtension
    static final OpenTelemetryExtension OTEL = OpenTelemetryExtension.create();

    private static final ObjectMapper JSON = new ObjectMapper();

    private static String traceparentOf(SpanContext context) {
        return "00-" + context.getTraceId() + "-" + context.getSpanId() + "-"
                + context.getTraceFlags().asHex();
    }

    private static EnqueueContext enqueue(FlexiQOtel middleware, EnqueueOptions options) {
        EnqueueContext context = new EnqueueContext("t", null, options);
        middleware.onEnqueue(context);
        return context;
    }

    private static TaskContext running(String metadataJson) throws Exception {
        Map<String, Object> metadata = JSON.readValue(metadataJson, Map.class);
        return new TaskContext("job-1", "t", new JobInfo("job-1", "t", () -> metadata));
    }

    @Test
    void anEnqueueCarriesTheCurrentSpan() throws Exception {
        FlexiQOtel middleware = new FlexiQOtel(OTEL.getOpenTelemetry());
        Span request =
                OTEL.getOpenTelemetry().getTracer("test").spanBuilder("request").startSpan();
        EnqueueContext context;
        try (Scope ignored = request.makeCurrent()) {
            context = enqueue(
                    middleware,
                    EnqueueOptions.builder().metadata("{\"tenant\":\"acme\"}").build());
        } finally {
            request.end();
        }

        Map<?, ?> metadata = JSON.readValue(context.options().metadata(), Map.class);
        assertEquals(traceparentOf(request.getSpanContext()), metadata.get("traceparent"));
        assertEquals("acme", metadata.get("tenant"), "the caller's own keys survive");
    }

    @Test
    void anEnqueueOutsideAnySpanLeavesMetadataAlone() {
        FlexiQOtel middleware = new FlexiQOtel(OTEL.getOpenTelemetry());
        assertNull(enqueue(middleware, EnqueueOptions.none()).options().metadata());
        assertEquals(
                "not json",
                enqueue(
                                middleware,
                                EnqueueOptions.builder().metadata("not json").build())
                        .options()
                        .metadata());
    }

    @Test
    void theExecuteSpanIsAChildOfTheEnqueuerAndCurrentForTheHandler() throws Exception {
        FlexiQOtel middleware = new FlexiQOtel(OTEL.getOpenTelemetry());
        String parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        TaskContext context = running("{\"traceparent\":\"" + parent + "\"}");

        middleware.before(context);
        SpanContext current = Span.current().getSpanContext();
        // A job the handler enqueues continues the execute span.
        EnqueueContext child = enqueue(middleware, EnqueueOptions.none());
        middleware.after(context, "ok");

        List<SpanData> spans = OTEL.getSpans();
        assertEquals(1, spans.size());
        SpanData execute = spans.get(0);
        assertEquals("flexiq.execute.t", execute.getName());
        assertEquals(SpanKind.CONSUMER, execute.getKind());
        assertEquals(StatusCode.OK, execute.getStatus().getStatusCode());
        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", execute.getTraceId());
        assertEquals("00f067aa0ba902b7", execute.getParentSpanId());
        assertEquals(execute.getSpanId(), current.getSpanId(), "the span is current for the handler");
        Map<?, ?> childMetadata = JSON.readValue(child.options().metadata(), Map.class);
        assertEquals(traceparentOf(current), childMetadata.get("traceparent"));
        assertTrue(!Span.current().getSpanContext().isValid(), "after() leaves the scope");
    }

    @Test
    void aSpanLeftCurrentOnTheThreadIsNeverTheParent() throws Exception {
        FlexiQOtel middleware = new FlexiQOtel(OTEL.getOpenTelemetry());
        Span stale =
                OTEL.getOpenTelemetry().getTracer("test").spanBuilder("stale").startSpan();
        TaskContext context = running("{}");

        try (Scope ignored = stale.makeCurrent()) {
            middleware.before(context);
            middleware.after(context, "ok");
        } finally {
            stale.end();
        }

        SpanData execute = OTEL.getSpans().stream()
                .filter(span -> span.getName().equals("flexiq.execute.t"))
                .findFirst()
                .orElseThrow();
        assertTrue(!execute.getParentSpanContext().isValid(), "a job with no carrier starts its own trace");
    }

    @Test
    void aSleptAttemptEndsWithNoStatus() throws Exception {
        FlexiQOtel middleware = new FlexiQOtel(OTEL.getOpenTelemetry());
        TaskContext context = running("{}");

        middleware.before(context);
        middleware.onSleep(context, 42L);

        SpanData span = OTEL.getSpans().get(0);
        assertEquals(StatusCode.UNSET, span.getStatus().getStatusCode());
        assertEquals("sleep", span.getEvents().get(0).getName());
    }

    @Test
    void aWorkerContinuesTheTraceEndToEnd(@TempDir Path dir) throws Exception {
        Task<String> task = Task.of("otel.e2e", String.class);
        Span request =
                OTEL.getOpenTelemetry().getTracer("test").spanBuilder("request").startSpan();
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("otel.db").toString()).open()) {
            queue.use(new FlexiQOtel(OTEL.getOpenTelemetry()));
            // A hook writing its own metadata keeps the injected context.
            queue.use(new Middleware() {
                @Override
                public void onEnqueue(EnqueueContext context) {
                    context.metadata().put("hook", "kept");
                }
            });
            String id;
            try (Scope ignored = request.makeCurrent()) {
                id = queue.enqueue(
                        task,
                        "payload",
                        EnqueueOptions.builder()
                                .metadata("{\"tenant\":\"acme\"}")
                                .build());
            } finally {
                request.end();
            }
            Map<?, ?> stored = JSON.readValue(queue.getJob(id).orElseThrow().metadata, Map.class);
            assertEquals(traceparentOf(request.getSpanContext()), stored.get("traceparent"));
            assertEquals("acme", stored.get("tenant"));
            assertEquals("kept", stored.get("hook"));

            AtomicReference<SpanContext> seen = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            try (Worker worker = queue.worker()
                    .handle(task, payload -> {
                        seen.set(Span.current().getSpanContext());
                        done.countDown();
                        return null;
                    })
                    .start()) {
                assertTrue(done.await(20, TimeUnit.SECONDS), "the job should run");
            }
            assertEquals(request.getSpanContext().getTraceId(), seen.get().getTraceId());
        }
    }
}
