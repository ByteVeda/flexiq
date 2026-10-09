package org.byteveda.flexiq.contrib;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.byteveda.flexiq.middleware.EnqueueContext;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.jspecify.annotations.Nullable;

/**
 * Wraps each task execution in an OpenTelemetry span, and carries the trace
 * across the queue.
 *
 * <p>An enqueue injects the current context into the job's metadata
 * ({@code traceparent}/{@code tracestate}, merged, never replacing the caller's
 * keys) through the instance's propagators. Execution extracts it, so the
 * {@code flexiq.execute.<task>} span is a child of the enqueuer — whichever SDK
 * enqueued the job. The span is current for the handler, so its own spans and
 * any job it enqueues continue the same trace.
 *
 * <p>Each attempt is one span: it ends {@code OK} in {@code after},
 * {@code ERROR} with the exception recorded in {@code onError}, and with no
 * status in {@code onSleep} — an attempt that slept neither succeeded nor failed.
 */
public final class FlexiQOtel implements Middleware {
    private static final String SPAN = "flexiq.contrib.otel.span";
    private static final String SCOPE = "flexiq.contrib.otel.scope";
    private static final String INSTRUMENTATION = "org.byteveda.flexiq";
    private static final AttributeKey<Long> WAKE_AT = AttributeKey.longKey("flexiq.wake_at");

    private static final TextMapSetter<Map<String, String>> SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.put(key, value);
        }
    };

    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public @Nullable String get(@Nullable Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final Predicate<String> taskFilter;

    /**
     * Trace every task.
     *
     * @param openTelemetry the application's instance, whose tracer provider and
     *     propagators decide where spans go and how context is written
     */
    public FlexiQOtel(OpenTelemetry openTelemetry) {
        this(openTelemetry, task -> true);
    }

    /**
     * Trace the tasks {@code taskFilter} accepts.
     *
     * @param openTelemetry the application's instance, whose tracer provider and
     *     propagators decide where spans go and how context is written
     * @param taskFilter which tasks to trace and propagate for, by task name
     */
    public FlexiQOtel(OpenTelemetry openTelemetry, Predicate<String> taskFilter) {
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION);
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
        this.taskFilter = taskFilter;
    }

    @Override
    public void onEnqueue(EnqueueContext context) {
        if (!taskFilter.test(context.taskName)) {
            return;
        }
        Map<String, String> carrier = new LinkedHashMap<>();
        propagator.inject(Context.current(), carrier, SETTER);
        String metadata = context.options().metadata();
        String merged = TraceCarrier.merge(metadata, carrier);
        if (merged != null && !merged.equals(metadata)) {
            context.options(context.options().toBuilder().metadata(merged).build());
        }
    }

    @Override
    public void before(TaskContext context) {
        if (!taskFilter.test(context.taskName)) {
            return;
        }
        // From the root, never the thread's current context: hooks close in
        // registration order, so a scope another middleware opened after this
        // one can leave a finished job's span current on the worker thread.
        Context parent = propagator.extract(
                Context.root(), TraceCarrier.extract(context.job().metadata()), GETTER);
        Span span = tracer.spanBuilder("flexiq.execute." + context.taskName)
                .setParent(parent)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("flexiq.job_id", context.jobId)
                .setAttribute("flexiq.task_name", context.taskName)
                .startSpan();
        context.attributes().put(SPAN, span);
        // Hooks and the handler share a thread, so the scope covers the handler.
        context.attributes().put(SCOPE, span.makeCurrent());
    }

    @Override
    public void after(TaskContext context, Object result) {
        Span span = close(context);
        if (span != null) {
            span.setStatus(StatusCode.OK);
            span.end();
        }
    }

    @Override
    public void onError(TaskContext context, Throwable error) {
        Span span = close(context);
        if (span != null) {
            span.recordException(error);
            span.setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
            span.end();
        }
    }

    @Override
    public void onSleep(TaskContext context, long wakeAt) {
        Span span = close(context);
        if (span != null) {
            span.setAttribute("flexiq.slept", true);
            span.addEvent("sleep", Attributes.of(WAKE_AT, wakeAt));
            span.end();
        }
    }

    /** Leave the span's scope, and hand back the span for its ending. */
    private static @Nullable Span close(TaskContext context) {
        Scope scope = (Scope) context.attributes().remove(SCOPE);
        if (scope != null) {
            scope.close();
        }
        return (Span) context.attributes().remove(SPAN);
    }
}
